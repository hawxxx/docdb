# docdb-segmentation-tool

Scripts for splitting a large DocumentDB collection into segments for an AWS DMS full load, and for checking source and target clusters stay in sync during the migration.

Tested on a collection of about 1.2 billion documents split into 8 segments.

## Files

| File | Purpose |
|------|---------|
| `docdb_segments.py` | Finds `_id` boundaries to split a collection into N segments, with one cursor or several in parallel |
| `compare_docdb.py` | Live terminal dashboard comparing old and new cluster stats and DMS CDC latency |
| `docdb_data_compare.py` | One-time consistency report: indexes, missing documents, field diffs, schema drift |
| `checkpoint_example.json` | Example checkpoint showing the file format |
| `WORKFLOW.md` | Diagram of how the scripts fit together |
| `research.md` | The math behind boundary search: quantiles, interpolation, sampling bounds, load balancing |
| `LICENSE`, `NOTICE` | Apache 2.0 license and attribution for the code taken from amazon-documentdb-tools |

## Requirements

Python 3.9+ with:

```bash
pip install pymongo boto3 rich python-dotenv deepdiff tqdm
```

`docdb_segments.py` needs pymongo 4.0 or later for `--parallel`. The other modes work with pymongo 3.10 or later, the same as the upstream tool.

`deepdiff` and `tqdm` are optional for `docdb_data_compare.py`. Without them it skips field-level diffs and progress bars.

AWS credentials are needed for the CDC latency sections (`dms:DescribeReplicationTasks`, `cloudwatch:GetMetricStatistics`).

## docdb_segments.py

Run with `--single-cursor` for large collections. Connect to an instance endpoint with `directConnection=true`, not the cluster endpoint:

```bash
python3 docdb_segments.py \
  --uri "mongodb://<user>:<password>@<instance-endpoint>:27017/?authSource=admin&tls=true&tlsCAFile=global-bundle.pem&directConnection=true" \
  --database <database> \
  --collection <collection> \
  --num-segments 8 \
  --single-cursor \
  --connect-timeout-ms 30000 \
  --socket-timeout-ms 600000 \
  --checkpoint-file checkpoint_<database>_<collection>_8.json
```

With `--single-cursor` the script walks the `_id` index in order and records a boundary every `count / num-segments` documents. Without it, it uses `skip()` to jump to each offset. On DocumentDB, `skip()` gets slow at high offsets, so the cursor mode is the better choice for big collections.

Progress is saved to the checkpoint file every 60 seconds and after each boundary. If the connection drops, run the same command again and it picks up where it stopped. The checkpoint is only reused if the database, collection, and segment count match. It is deleted when the run finishes.

`--resume-from` takes a comma-separated list of known boundary `_id` values if you want to start from boundaries you already have.

The script stops early if the first and last `_id` have different types. Supported `_id` types are `int`, `string`, and `objectId`.

When it finishes it prints the boundaries in two formats: a plain comma-separated list, and the bracketed format used in a DMS table mapping segmentation rule.

### Checkpoint fields

| Field | Meaning |
|---|---|
| `boundaries` | Boundary `_id` values found so far |
| `numSegments` | Requested segment count |
| `thisBoundary` | Number of boundaries found so far |
| `numDocuments` | Collection document count at start |
| `feedbackDocuments` | Documents per segment |
| `numDocsTotal` | Documents scanned so far |
| `numDocsBoundary` | Documents scanned since the last boundary |
| `startFromId`, `lastId` | `_id` to resume from |


`checkpoint_example.json` shows the format of a run that stopped after 3 of 7 boundaries. The values are made up.

### Parallel mode

`--parallel N` scans with N cursors at once (1 to 8). It works for `objectId` and `int` `_id` values. For `string` `_id`, use `--single-cursor`.

```bash
python3 docdb_segments.py \
  --uri "mongodb://<user>:<password>@<replica-instance-endpoint>:27017/?authSource=admin&tls=true&tlsCAFile=global-bundle.pem&directConnection=true" \
  --database <database> \
  --collection <collection> \
  --num-segments 8 \
  --parallel 4 \
  --max-docs-per-sec 20000
```

How it works: it reads the first and last `_id`, splits the key range into `N x 4` pieces by interpolating between them (ObjectId timestamp or integer value), and hands those pieces to the workers from a queue. Each worker walks its piece in `_id` order and notes an `_id` every so often. At the end the counts are added up and each boundary is set to the nearest noted `_id`. The pieces don't need to be the same size for this to work. Uneven pieces only mean some workers finish sooner.

Boundaries are placed to within about 0.1% of a segment, which is plenty for DMS. If you need them tighter, lower `--marker-interval`.

| Option | Default | Description |
|---|---|---|
| `--parallel` | off | Number of workers, 1 to 8 |
| `--max-docs-per-sec` | 20000 | Total read rate across all workers, 0 for no limit |
| `--batch-size` | 5000 | Cursor batch size |
| `--read-preference` | `secondary` | `secondaryPreferred` when `--allow-primary` is set. `primary` and `primaryPreferred` need `--allow-primary` |
| `--allow-primary` | off | Allow running when reads would go to the primary |
| `--marker-interval` | auto | Note an `_id` every N documents (about 0.1% of a segment) |
| `--checkpoint-file` | `checkpoint_<database>_<collection>_<n>_parallel.json` | Where progress is saved |

Limits on how hard it hits the database:

- Reads go to a replica (`readPreference=secondary`) and never fall back to the primary. If the only reachable instance is the primary, the script stops before reading anything. You can override that with `--allow-primary`, but don't do it on a production cluster.
- `--max-docs-per-sec` caps the total across all workers. It defaults to 20000. Set it to 0 to remove the limit.
- At most 8 workers, and the connection pool is sized to the worker count.
- Each query is a range scan on the `_id` index (hinted) that returns only `_id`.
- The app name is `segmentr-parallel`, so the queries are easy to find in `db.currentOp()` and kill if needed.

For DocumentDB, the simplest setup is a replica instance endpoint with `directConnection=true`. That's the same kind of connection the other modes use, and every query stays on that one replica. Alternatively, use the cluster endpoint with `replicaSet=rs0` so the driver can pick a replica. The cluster needs at least one replica instance either way. Start with a low `--max-docs-per-sec` and watch `CPUUtilization` and `ReadIOPS` for the replica in CloudWatch before raising it.

Errors on a range are retried up to 8 times with backoff, resuming from the last `_id` read. Progress is written to `checkpoint_<database>_<collection>_<n>_parallel.json` every 60 seconds, on Ctrl+C, and on failure. Run the same command again to continue. The checkpoint stays on disk after a successful run, and rerunning prints the saved result without scanning again. Delete the file to start a fresh scan. Cursor-mode and parallel-mode checkpoints use different formats and can't be swapped.

Documents inserted into a range after it's been scanned aren't counted. That's fine for boundaries, since DMS loads every document regardless of where the boundaries fall.

## compare_docdb.py

Configured through environment variables or a `.env` file:

```
OLD_HOST=
OLD_USER=
OLD_PASS=
OLD_SSL=false
NEW_HOST=
NEW_USER=
NEW_PASS=
NEW_SSL=true
DBS=<db1>,<db2>
REFRESH_INTERVAL=5
DMS_TASK_IDENTIFIERS=
DMS_REPLICATION_INSTANCE_IDENTIFIER=
SSL_CA_FILE=global-bundle.pem
```

```bash
python3 compare_docdb.py
```

Shows count, size, storage, index count and size, scan counts, and insert/update/delete counters for each collection on both clusters, refreshed on an interval. Arrows show change since the last refresh. A CDC latency table at the top shows source and target latency per DMS task from CloudWatch. Exit with Ctrl+C.

## docdb_data_compare.py

```bash
python3 docdb_data_compare.py --databases <database> --collections <collection> --sample-size-percent 1 --sample-anchor-source
```

Checks per collection:

- Collection exists on both clusters
- Index names, keys, `unique`, and `sparse` match
- Every `_id` exists on both sides (full scan, or a sample with `--sample-size-percent`)
- Documents with the same `_id` have the same content (field-level diff with `deepdiff`)
- Field names and types match across a sample of documents

Useful options:

| Option | Default | Description |
|---|---|---|
| `--sample-size-percent` | 100 | Percent of documents to check. 100 is a full scan |
| `--sample-anchor-source` | off | Sample IDs from the source and look up the same IDs on the target |
| `--check-ids` | none | Specific `_id` values to always check |
| `--diff-limit` | 50 | Max differing documents recorded per collection |
| `--skip-deep-diff` | off | Skip field-level diffs |
| `--output-file` | none | Append missing IDs and diffs to a file |
| `--no-latency` | off | Skip the DMS CDC latency lookup |

Full scans on collections with hundreds of millions of documents take a long time. Start with a small anchored sample.

It reads the same environment variables as `compare_docdb.py` (`OLD_*`, `NEW_*`, `SSL_CA_FILE`, `DBS`, `DMS_TASK_IDENTIFIERS`, `DMS_REPLICATION_INSTANCE_IDENTIFIER`). `--databases` overrides `DBS`.

## Credentials

No credentials are stored in these files. Put them in a `.env` file next to the scripts or export them in your shell, and keep `.env` out of version control.

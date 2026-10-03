# DocDB Segments

Finds `_id` boundaries that divide a DocumentDB collection into balanced migration segments. It supports a single cursor, parallel range scans, and resumable checkpoints.

## Usage

From the repository root:

```bash
python3 docdb-segments/docdb_segments.py \
  --uri 'mongodb://user:password@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' \
  --database my_database \
  --collection my_collection \
  --num-segments 8 \
  --single-cursor
```

For parallel scanning, replace `--single-cursor` with `--parallel N`. Use `--resume-from PATH` to continue from a checkpoint. Run `python3 docdb-segments/docdb_segments.py --help` for every option.

The included [checkpoint_example.json](checkpoint_example.json) shows the checkpoint format.

## Requirements

Install the shared dependencies from the repository root:

```bash
pip install -r requirements.txt
```

Parallel mode requires PyMongo 4.0 or later.

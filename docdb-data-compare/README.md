# DocDB Data Compare

Creates a one-time consistency report for two DocumentDB clusters. It compares collections, indexes, document IDs, optional field-level differences, schema samples, and optional AWS DMS latency.

## Configuration

Use the same `OLD_*`, `NEW_*`, `SSL_CA_FILE`, `DBS`, and optional DMS environment variables documented by [Compare DocDB](../compare-docdb/README.md).

## Usage

From the repository root:

```bash
python3 docdb-data-compare/docdb_data_compare.py \
  --databases my_database \
  --collections my_collection \
  --sample-size-percent 1 \
  --sample-anchor-source
```

Run `python3 docdb-data-compare/docdb_data_compare.py --help` for sampling, output, ID checking, and deep-diff options.

## Requirements

```bash
pip install -r requirements.txt
```

`deepdiff` enables field-level comparisons and `tqdm` enables progress bars.

# DocumentDB migration tools

Three standalone Python tools for planning and validating Amazon DocumentDB migrations.

| Tool | Purpose |
| --- | --- |
| [DocDB Segments](docdb-segments/) | Generate `_id` boundaries for parallel migration segments |
| [Compare DocDB](compare-docdb/) | Monitor source and target clusters in a live terminal dashboard |
| [DocDB Data Compare](docdb-data-compare/) | Produce a one-time data consistency report |

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Each tool has its own README with configuration and usage examples. See [WORKFLOW.md](WORKFLOW.md) for an end-to-end migration workflow.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

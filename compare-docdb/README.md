# Compare DocDB

Displays a live terminal dashboard comparing collection statistics from source and target DocumentDB clusters, with optional AWS DMS CDC latency.

## Configuration

Create a `.env` file in the directory where you run the command:

```dotenv
OLD_HOST=source.example.com
OLD_PORT=27017
OLD_USER=username
OLD_PASS=password
OLD_SSL=true
NEW_HOST=target.example.com
NEW_PORT=27017
NEW_USER=username
NEW_PASS=password
NEW_SSL=true
DBS=my_database
SSL_CA_FILE=global-bundle.pem
REFRESH_INTERVAL=5
AWS_REGION=us-east-1
DMS_TASK_IDENTIFIERS=
DMS_REPLICATION_INSTANCE_IDENTIFIER=
```

## Usage

From the repository root:

```bash
python3 compare-docdb/compare_docdb.py
```

Install dependencies first with `pip install -r requirements.txt`. AWS credentials are required only when DMS latency monitoring is configured.

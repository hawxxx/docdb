# DMS Segmentation & Comparison Workflow

Visual reference for how `docdb_segments.py`, `compare_docdb.py`, and
`docdb_data_compare.py` fit together in a DMS migration. See [README.md](README.md) for full script-level detail.

```mermaid
flowchart TD
    A["Start: source collection<br/>~1.2B documents"] --> B["docdb_segments.py<br/>--single-cursor --num-segments 8"]
    A --> PA["docdb_segments.py<br/>--parallel N --num-segments 8"]
    PA --> PB{"Reads would hit the primary?"}
    PB -->|Yes, no --allow-primary| PX["Stop before reading data"]
    PB -->|No| PC["Read first and last _id<br/>interpolate N x 4 key ranges"]
    PC --> PD["Workers scan ranges from a queue<br/>replica reads, shared rate limit<br/>retry and checkpoint per range"]
    PD --> PE["Merge counts and markers<br/>pick nearest _id for each boundary"]
    PE --> O

    B --> C{"Checkpoint file exists?<br/>checkpoint file"}
    C -->|Yes, matches db/collection/segments| D["Load checkpoint<br/>Resume from lastId"]
    C -->|No| E["Fresh start<br/>find first _id ascending"]

    D --> F["Open cursor sorted by _id ASC<br/>filter: _id greater than startFromId"]
    E --> F

    F --> G["Walk cursor document by document"]
    G --> H{"Connection/timeout error?"}
    H -->|Yes| I["retry_with_backoff<br/>exponential retry, then save checkpoint on failure"]
    I --> G
    H -->|No| J{"numDocsBoundary >= feedbackDocuments?"}

    J -->|No| K["Increment counters<br/>print progress every 10s<br/>save checkpoint every 60s"]
    K --> G

    J -->|Yes| L["Record boundary _id<br/>append to boundaries list"]
    L --> M{"thisBoundary >= numSegments - 1?"}
    M -->|No| G
    M -->|Yes| N["All 8 boundaries found<br/>print DMS-ready boundary list<br/>delete checkpoint file"]

    N --> O["Paste boundaries into<br/>AWS DMS table-segmentation config"]
    O --> P["Run DMS full-load task<br/>8 parallel segments"]

    P --> Q["compare_docdb.py<br/>live rich dashboard"]
    P --> R["docdb_data_compare.py<br/>one-shot consistency report"]

    Q --> Q1["Collection stats OLD vs NEW<br/>count/size/storage/indexes/ops<br/>refreshed every 5s"]
    Q --> Q2["CDC latency via CloudWatch<br/>CDCLatencySource / CDCLatencyTarget"]

    R --> R1["Index diff<br/>missing/extra/changed"]
    R --> R2["Document _id set diff<br/>full scan or sample percent"]
    R --> R3["Field-level deep diff<br/>via deepdiff, capped by diff-limit"]
    R --> R4["Schema drift<br/>field presence and type mismatches"]
    R --> R5["Final rich report<br/>summary + per-collection recommendations"]

    style N fill:#2c5282,color:#fff
    style A fill:#2c5282,color:#fff
    style P fill:#9b2c2c,color:#fff
```

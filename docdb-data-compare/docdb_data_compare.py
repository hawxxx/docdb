#!/usr/bin/env python3
"""
DocumentDB data consistency checker.

Built from the existing compare_docdb.py helper and the Amazon
DocumentDB DataDiffer reference tool. It performs:
  - Collection presence checks
  - Index comparison
  - Document existence checks (both directions)
  - Optional document deep-diff (field-level) using DeepDiff

Connection settings are read from environment variables (or a .env
file), the same as compare_docdb.py.
"""

import argparse
import json
import os
import ssl
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pymongo

try:
    from deepdiff import DeepDiff  # type: ignore
except Exception:
    DeepDiff = None  # Deep diff is optional

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # Progress bars optional

try:
    from rich.console import Console  # type: ignore
    from rich.table import Table  # type: ignore
    from rich import box  # type: ignore
except Exception:
    Console = None
    Table = None
    box = None

try:
    import boto3  # type: ignore
    from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
except Exception:
    boto3 = None
    BotoCoreError = ClientError = None

# --- Connection configuration (environment variables or a .env file) ---
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "t", "yes", "y", "on")


OLD_HOST = os.getenv("OLD_HOST", "")
OLD_PORT = int(os.getenv("OLD_PORT", "27017"))
OLD_USER = os.getenv("OLD_USER", "")
OLD_PASS = os.getenv("OLD_PASS", "")
OLD_SSL = _env_bool("OLD_SSL", False)

NEW_HOST = os.getenv("NEW_HOST", "")
NEW_PORT = int(os.getenv("NEW_PORT", "27017"))
NEW_USER = os.getenv("NEW_USER", "")
NEW_PASS = os.getenv("NEW_PASS", "")
NEW_SSL = _env_bool("NEW_SSL", True)

SSL_CA_FILE = os.getenv("SSL_CA_FILE", "global-bundle.pem")

# Databases to compare by default
DEFAULT_DATABASES = [d.strip() for d in os.getenv("DBS", "").split(",") if d.strip()]

# AWS / DMS latency configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
# Friendly task names or ARNs to include (empty => all tasks)
DMS_TASK_IDENTIFIERS = [t.strip() for t in os.getenv("DMS_TASK_IDENTIFIERS", "").split(",") if t.strip()]
# Replication instance identifier (or ARN) used for CloudWatch latency metrics
DMS_REPLICATION_INSTANCE_IDENTIFIER = os.getenv("DMS_REPLICATION_INSTANCE_IDENTIFIER", "")

# --- Helpers -----------------------------------------------------------------


def get_client(host: str, port: int, user: str, password: str, ssl_enabled: bool) -> Optional[pymongo.MongoClient]:
    """Create and return a Mongo client, or None on failure."""
    try:
        if ssl_enabled:
            client = pymongo.MongoClient(
                host,
                port=port,
                username=user,
                password=password,
                ssl=True,
                ssl_ca_certs=SSL_CA_FILE,
                ssl_cert_reqs=ssl.CERT_REQUIRED,
                serverSelectionTimeoutMS=5000,
            )
        else:
            uri = f"mongodb://{user}:{password}@{host}:{port}/"
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client
    except Exception as exc:  # noqa: BLE001
        print(f"[connect] Failed connecting to {host}:{port}: {exc}", file=sys.stderr)
        return None


def write_output(output_file: Optional[str], content):
    if not output_file:
        return
    try:
        with open(output_file, "a", encoding="utf-8") as fh:
            fh.write(f"{content}\n")
    except Exception:
        pass


# --- AWS / DMS helpers -------------------------------------------------------


def extract_identifier_from_arn(arn: str) -> Optional[str]:
    if not arn:
        return None
    parts = arn.split(":", 5)
    if len(parts) < 6:
        return arn
    resource = parts[5]
    if ":" in resource:
        resource = resource.split(":", 1)[1]
    if "/" in resource:
        resource = resource.split("/", 1)[1]
    return resource


def normalize_task_identifier(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("arn:"):
        return extract_identifier_from_arn(value)
    return value


def normalize_replication_instance(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("arn:"):
        value = extract_identifier_from_arn(value) or value
    if value.startswith("rep:"):
        value = value.split(":", 1)[1]
    return value


aws_session = None
dms_client = None
cloudwatch_client = None
_cdc_debug_logged = False


def init_aws_clients() -> bool:
    global aws_session, dms_client, cloudwatch_client
    if not boto3:
        return False
    if dms_client and cloudwatch_client:
        return True
    try:
        aws_session = boto3.Session(region_name=AWS_REGION)
        dms_client = aws_session.client("dms")
        cloudwatch_client = aws_session.client("cloudwatch")
        return True
    except Exception:
        return False


def fetch_cloudwatch_latency(instance_id: str, task_id: str, metric_name: str) -> Optional[float]:
    if not init_aws_clients():
        return None
    if not instance_id or not task_id:
        return None

    instance_id = normalize_replication_instance(instance_id) or instance_id
    task_id = normalize_task_identifier(task_id) or task_id

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    try:
        response = cloudwatch_client.get_metric_statistics(
            Namespace="AWS/DMS",
            MetricName=metric_name,
            Dimensions=[
                {"Name": "ReplicationInstanceIdentifier", "Value": instance_id},
                {"Name": "ReplicationTaskIdentifier", "Value": task_id},
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,
            Statistics=["Average"],
        )
    except Exception:
        return None

    datapoints = response.get("Datapoints", [])
    if not datapoints:
        global _cdc_debug_logged
        if not _cdc_debug_logged:
            print(f"[cdc] No datapoints for instance={instance_id} task={task_id} metric={metric_name}")
            _cdc_debug_logged = True
        return None
    latest = max(datapoints, key=lambda x: x["Timestamp"])
    return latest.get("Average")


def get_cdc_latency_data() -> List[Dict]:
    if not init_aws_clients():
        return []

    tasks = []
    try:
        paginator = dms_client.get_paginator("describe_replication_tasks")
        for page in paginator.paginate():
            tasks.extend(page.get("ReplicationTasks", []))
        if DMS_TASK_IDENTIFIERS:
            normalized_ids = set(normalize_task_identifier(t) for t in DMS_TASK_IDENTIFIERS if t)
            tasks = [t for t in tasks if t.get("ReplicationTaskIdentifier") in normalized_ids]
    except Exception:
        return []

    latencies = []
    for task in tasks:
        task_identifier = task.get("ReplicationTaskIdentifier")
        task_arn = task.get("ReplicationTaskArn")
        task_status = task.get("Status", "unknown")
        migration_type = task.get("MigrationType", "-")

        if DMS_REPLICATION_INSTANCE_IDENTIFIER:
            instance_id = DMS_REPLICATION_INSTANCE_IDENTIFIER
        else:
            instance_arn = task.get("ReplicationInstanceArn")
            instance_id = normalize_replication_instance(instance_arn) if instance_arn else None

        task_id_for_cw = None
        if task_arn:
            task_id_for_cw = extract_identifier_from_arn(task_arn)
        elif task_identifier and len(task_identifier) > 20:
            task_id_for_cw = task_identifier
        else:
            task_id_for_cw = task_identifier

        source_latency = fetch_cloudwatch_latency(instance_id, task_id_for_cw, "CDCLatencySource") if instance_id else None
        target_latency = fetch_cloudwatch_latency(instance_id, task_id_for_cw, "CDCLatencyTarget") if instance_id else None

        latencies.append(
            {
                "task_id": task_identifier,
                "status": task_status,
                "migration_type": migration_type,
                "source_latency": source_latency,
                "target_latency": target_latency,
                "instance": instance_id,
            }
        )
    return latencies


def chunked(seq: List, size: int) -> List[List]:
    """Yield list chunks of given size."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def summarize_index(index_doc: Dict) -> Dict:
    """Return a simplified index definition for comparison."""
    keys = list(index_doc.get("key", {}).items())
    return {
        "name": index_doc.get("name"),
        "keys": keys,
        "unique": bool(index_doc.get("unique", False)),
        "sparse": bool(index_doc.get("sparse", False)),
    }


def diff_indexes(source_indexes: List[Dict], target_indexes: List[Dict]) -> Dict:
    """Compare index lists and return differences."""
    src_map = {idx["name"]: idx for idx in source_indexes}
    tgt_map = {idx["name"]: idx for idx in target_indexes}

    missing_in_target = [src_map[name] for name in src_map.keys() - tgt_map.keys()]
    extra_in_target = [tgt_map[name] for name in tgt_map.keys() - src_map.keys()]

    changed = []
    for name in src_map.keys() & tgt_map.keys():
        if src_map[name] != tgt_map[name]:
            changed.append({"name": name, "source": src_map[name], "target": tgt_map[name]})

    return {
        "missing_in_target": missing_in_target,
        "extra_in_target": extra_in_target,
        "changed": changed,
    }


def normalize_id(value):
    """Make _id hashable; keep original for reporting."""
    if isinstance(value, dict):
        try:
            return json.dumps(value, sort_keys=True)
        except Exception:
            return str(value)
    return value


def parse_explicit_ids(value: Optional[str]) -> List:
    """Parse comma-separated or JSON array of _id values."""
    if not value:
        return []
    value = value.strip()
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    # fallback: comma separated
    return [v.strip() for v in value.split(",") if v.strip()]


def fetch_ids(coll, batch_size: int, sample_percent: float, timeout_ms: int) -> Tuple[List, int, Dict]:
    """
    Return _id values (hashable) from a collection (full or sampled) and the estimated total count.

    - sample_percent >= 100 fetches all ids (sorted) like the original tool.
    - Otherwise, uses aggregation $sample with maxTimeMS; falls back to find+limit
      if $sample is not available.
    """
    total = 0
    try:
        total = coll.estimated_document_count()
    except Exception:
        pass

    # If we can't estimate, fall back to full scan
    if total == 0:
        sample_size = None
    else:
        sample_size = int(max(1, (sample_percent / 100.0) * total))

    ids: List = []
    if sample_size is None or sample_percent >= 100:
        cursor = coll.find({}, {"_id": 1}).sort("_id", 1).batch_size(batch_size)
    else:
        try:
            cursor = coll.aggregate(
                [
                    {"$sample": {"size": sample_size}},
                    {"$project": {"_id": 1}},
                ],
                allowDiskUse=True,
                maxTimeMS=timeout_ms,
            )
        except Exception:
            cursor = coll.find({}, {"_id": 1}, max_time_ms=timeout_ms).limit(sample_size)

    id_lookup = {}
    for doc in cursor:
        raw_id = doc["_id"]
        norm_id = normalize_id(raw_id)
        ids.append(doc["_id"])
        id_lookup[norm_id] = raw_id
    return ids, total, id_lookup


def analyze_schema_samples(coll, sample_percent: float, timeout_ms: int, sample_cap: int = 2000) -> Dict[str, set]:
    """
    Inspect sampled documents and return a mapping of field path -> set of types.
    Uses $sample when possible to avoid collection scans.
    """
    try:
        total = coll.estimated_document_count()
    except Exception:
        total = 0
    sample_size = sample_cap
    if total:
        sample_size = int(max(1, min(sample_cap, (sample_percent / 100.0) * total)))

    try:
        cursor = coll.aggregate(
            [{"$sample": {"size": sample_size}}],
            allowDiskUse=True,
            maxTimeMS=timeout_ms,
        )
    except Exception:
        cursor = coll.find({}, max_time_ms=timeout_ms).limit(sample_size)

    schema: Dict[str, set] = defaultdict(set)

    def walk(doc, prefix=""):
        if isinstance(doc, dict):
            for k, v in doc.items():
                path = f"{prefix}.{k}" if prefix else k
                schema[path].add(type(v).__name__)
                walk(v, path)
        elif isinstance(doc, list):
            path = prefix + "[]"
            schema[path].add("list")
            for item in doc:
                walk(item, path)

    for doc in cursor:
        walk(doc)

    return schema


def deep_diff_docs(old_doc: Dict, new_doc: Dict) -> Dict:
    """Return a diff between two documents (or an empty dict)."""
    if DeepDiff is None:
        return {"message": "DeepDiff not installed; documents differ."} if old_doc != new_doc else {}
    diff = DeepDiff(old_doc, new_doc, ignore_order=True, report_repetition=True)
    return diff.to_dict() if hasattr(diff, "to_dict") else diff


def compare_documents(
    old_coll,
    new_coll,
    batch_size: int,
    diff_limit: int,
    enable_deep_diff: bool,
    sample_percent: float,
    timeout_ms: int,
    progress: bool,
    output_file: Optional[str],
    label: Optional[str] = None,
    explicit_ids: Optional[List] = None,
    anchor_sample: bool = False,
) -> Dict:
    """Compare documents between two collections."""
    missing_in_new: List = []
    missing_in_old: List = []
    diffs: List[Tuple] = []

    old_ids, old_total, old_lookup = fetch_ids(old_coll, batch_size, sample_percent, timeout_ms)
    if anchor_sample:
        anchor_raw_ids = list(old_lookup.values())
        new_ids = []
        new_lookup = {}
        try:
            cursor = new_coll.find({"_id": {"$in": anchor_raw_ids}}, {"_id": 1})
            for doc in cursor:
                rid = doc["_id"]
                norm = normalize_id(rid)
                new_ids.append(rid)
                new_lookup[norm] = rid
        except Exception:
            pass
        try:
            new_total = new_coll.estimated_document_count()
        except Exception:
            new_total = len(new_ids)
    else:
        new_ids, new_total, new_lookup = fetch_ids(new_coll, batch_size, sample_percent, timeout_ms)

    old_id_set = set(old_ids)
    new_id_set = set(new_ids)

    explicit_ids = explicit_ids or []
    if explicit_ids:
        for raw in explicit_ids:
            norm = normalize_id(raw)
            # fetch presence explicitly
            if norm not in old_id_set:
                doc = old_coll.find_one({"_id": raw})
                if doc is not None:
                    old_id_set.add(norm)
                    old_lookup[norm] = raw
            if norm not in new_id_set:
                doc = new_coll.find_one({"_id": raw})
                if doc is not None:
                    new_id_set.add(norm)
                    new_lookup[norm] = raw

    missing_in_new = [old_lookup[i] for i in sorted(old_id_set - new_id_set)]
    missing_in_old = [new_lookup[i] for i in sorted(new_id_set - old_id_set)]
    if missing_in_new:
        write_output(output_file, f"{len(missing_in_new)} docs missing in NEW")
        for mid in missing_in_new:
            write_output(output_file, f"missing_in_new: {mid}")
    if missing_in_old:
        write_output(output_file, f"{len(missing_in_old)} docs missing in OLD")
        for mid in missing_in_old:
            write_output(output_file, f"missing_in_old: {mid}")

    shared_ids = sorted(old_id_set & new_id_set)

    pbar = None
    if tqdm and progress and shared_ids:
        desc = f"Comparing {label}" if label else "Comparing docs"
        pbar = tqdm(total=len(shared_ids), desc=desc, unit="doc")

    for chunk in chunked(shared_ids, batch_size):
        raw_ids = [old_lookup.get(i, i) for i in chunk]
        old_docs = {normalize_id(d["_id"]): d for d in old_coll.find({"_id": {"$in": raw_ids}})}
        new_docs = {normalize_id(d["_id"]): d for d in new_coll.find({"_id": {"$in": raw_ids}})}

        for norm_id in chunk:
            if len(diffs) >= diff_limit:
                if pbar:
                    pbar.close()
                return {
                    "missing_in_new": missing_in_new,
                    "missing_in_old": missing_in_old,
                    "diffs": diffs,
                    "diffs_truncated": True,
                    "old_total": old_total,
                    "new_total": new_total,
                    "sample_percent": sample_percent,
                }
            o_doc = old_docs.get(norm_id)
            n_doc = new_docs.get(norm_id)
            if o_doc is None or n_doc is None:
                continue

            if o_doc != n_doc:
                diff_payload = deep_diff_docs(o_doc, n_doc) if enable_deep_diff else {"message": "Different content"}
                diff_id = old_lookup.get(norm_id, norm_id)
                diffs.append((diff_id, diff_payload))
                write_output(output_file, f"diff {diff_id} -> {diff_payload}")
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    return {
        "missing_in_new": missing_in_new,
        "missing_in_old": missing_in_old,
        "diffs": diffs,
        "diffs_truncated": len(shared_ids) > 0 and len(diffs) >= diff_limit,
        "old_total": old_total,
        "new_total": new_total,
        "sample_percent": sample_percent,
    }


def compare_collection(
    db_name: str,
    coll_name: str,
    old_client,
    new_client,
    batch_size: int,
    diff_limit: int,
    enable_deep_diff: bool,
    sample_percent: float,
    timeout_ms: int,
    progress: bool,
    output_file: Optional[str],
    explicit_ids: Optional[List],
    anchor_sample: bool,
) -> Dict:
    start_ts = datetime.now(timezone.utc)
    result = {
        "collection": coll_name,
        "status": "ok",
        "issues": [],
        "indexes": {},
        "documents": {},
        "schema": {},
        "duration_sec": 0.0,
    }

    old_coll = old_client[db_name][coll_name] if old_client else None
    new_coll = new_client[db_name][coll_name] if new_client else None

    if old_coll is None or new_coll is None:
        result["status"] = "missing"
        if old_coll and not new_coll:
            result["issues"].append("Collection missing in NEW")
        elif new_coll and not old_coll:
            result["issues"].append("Collection missing in OLD")
        else:
            result["issues"].append("Collection missing in both clusters")
        return result

    # Index comparison
    try:
        old_indexes = [summarize_index(i) for i in old_coll.list_indexes()]
        new_indexes = [summarize_index(i) for i in new_coll.list_indexes()]
        result["indexes"] = diff_indexes(old_indexes, new_indexes)
        if any(result["indexes"].values()):
            result["issues"].append("Index differences detected")
    except Exception as exc:  # noqa: BLE001
        result["issues"].append(f"Index check failed: {exc}")

    # Document comparison
    try:
        skip_docs = False
        try:
            old_count_est = old_coll.estimated_document_count()
            new_count_est = new_coll.estimated_document_count()
            if (old_count_est == 0) and (new_count_est == 0):
                skip_docs = True
                result["documents"] = {
                    "missing_in_new": [],
                    "missing_in_old": [],
                    "diffs": [],
                    "diffs_truncated": False,
                    "old_total": 0,
                    "new_total": 0,
                    "sample_percent": sample_percent,
                }
                result["issues"].append("Skipped doc compare (both collections empty)")
        except Exception:
            pass

        if not skip_docs:
            doc_diff = compare_documents(
                old_coll,
                new_coll,
                batch_size,
                diff_limit,
                enable_deep_diff,
                sample_percent,
                timeout_ms,
                progress,
                output_file,
                label=f"{db_name}.{coll_name}",
            explicit_ids=explicit_ids,
            anchor_sample=anchor_sample,
            )
            result["documents"] = doc_diff
            if doc_diff["missing_in_new"]:
                result["issues"].append(f"{len(doc_diff['missing_in_new'])} docs missing in NEW")
            if doc_diff["missing_in_old"]:
                result["issues"].append(f"{len(doc_diff['missing_in_old'])} docs missing in OLD")
            if doc_diff["diffs"]:
                result["issues"].append(f"{len(doc_diff['diffs'])} docs differ")
            if doc_diff.get("diffs_truncated"):
                result["issues"].append("Diff list truncated")
    except Exception as exc:  # noqa: BLE001
        result["issues"].append(f"Document comparison failed: {exc}")

    # Schema comparison on samples from both clusters
    try:
        old_schema = analyze_schema_samples(old_coll, sample_percent, timeout_ms)
        new_schema = analyze_schema_samples(new_coll, sample_percent, timeout_ms)
        missing_fields = sorted(set(old_schema.keys()) - set(new_schema.keys()))
        extra_fields = sorted(set(new_schema.keys()) - set(old_schema.keys()))
        type_mismatches = []
        for field in old_schema.keys() & new_schema.keys():
            if old_schema[field] != new_schema[field]:
                type_mismatches.append(
                    {"field": field, "old_types": sorted(old_schema[field]), "new_types": sorted(new_schema[field])}
                )
        result["schema"] = {
            "missing_in_new": missing_fields,
            "extra_in_new": extra_fields,
            "type_mismatches": type_mismatches,
        }
        if missing_fields:
            result["issues"].append(f"{len(missing_fields)} fields missing in NEW (schema sample)")
        if extra_fields:
            result["issues"].append(f"{len(extra_fields)} fields new in NEW (schema sample)")
        if type_mismatches:
            result["issues"].append(f"{len(type_mismatches)} field type mismatches")
    except Exception as exc:  # noqa: BLE001
        result["issues"].append(f"Schema comparison failed: {exc}")

    if not result["issues"]:
        result["issues"].append("No issues detected")
    result["duration_sec"] = (datetime.now(timezone.utc) - start_ts).total_seconds()
    return result


def compare_database(
    db_name: str,
    old_client,
    new_client,
    batch_size: int,
    diff_limit: int,
    collections: Optional[List[str]],
    enable_deep_diff: bool,
    sample_percent: float,
    timeout_ms: int,
    progress: bool,
    output_file: Optional[str],
    explicit_ids: Optional[List],
    anchor_sample: bool,
) -> Dict:
    print(f"\n=== Comparing database: {db_name} ===")
    old_db = old_client[db_name] if old_client else None
    new_db = new_client[db_name] if new_client else None

    if not old_db:
        print(f"Old database {db_name} not available")
        return {}

    old_colls = set(old_db.list_collection_names())
    new_colls = set(new_db.list_collection_names()) if new_db else set()

    if collections:
        target_colls = set(collections)
    else:
        target_colls = old_colls | new_colls

    db_result = {}
    for coll_name in sorted(target_colls):
        coll_result = compare_collection(
            db_name,
            coll_name,
            old_client,
            new_client,
            batch_size,
            diff_limit,
            enable_deep_diff,
            sample_percent,
            timeout_ms,
            progress,
            output_file,
            explicit_ids,
            anchor_sample,
        )
        db_result[coll_name] = coll_result

        issue_summary = "; ".join(coll_result["issues"])
        status_text = coll_result["status"]
        if Console:
            color = "green"
            if status_text.lower() == "missing":
                color = "red"
            elif status_text.lower() != "ok":
                color = "yellow"
            console = Console()
            console.print(f"[{color}][{status_text}][/]{' '}{db_name}.{coll_name}: {issue_summary}")
        else:
            print(f"[{status_text}] {db_name}.{coll_name}: {issue_summary}")

    return db_result


def summarize_results(all_results: Dict):
    summary = defaultdict(int)
    samples = {
        "missing_new": [],
        "missing_old": [],
        "index_missing": [],
        "index_extra": [],
        "index_changed": [],
        "doc_diffs": [],
    }
    per_collection_recs = defaultdict(list)
    for db_name, coll_results in all_results.items():
        for coll_name, result in coll_results.items():
            summary["collections_total"] += 1
            issues = result.get("issues", [])
            docs = result.get("documents", {})
            idx = result.get("indexes", {})
            schema = result.get("schema", {})

            missing_new = len(docs.get("missing_in_new", []) or [])
            missing_old = len(docs.get("missing_in_old", []) or [])
            diffs = len(docs.get("diffs", []) or [])
            idx_diffs = len(idx.get("missing_in_target", []) or []) + len(idx.get("extra_in_target", []) or []) + len(
                idx.get("changed", []) or []
            )
            schema_diffs = len(schema.get("missing_in_new", []) or []) + len(schema.get("extra_in_new", []) or []) + len(
                schema.get("type_mismatches", []) or []
            )

            if issues and issues == ["No issues detected"]:
                summary["collections_ok"] += 1
            elif "Collection missing" in ";".join(issues):
                summary["collections_missing"] += 1
            else:
                summary["collections_with_issues"] += 1

            summary["docs_missing_new"] += missing_new
            summary["docs_missing_old"] += missing_old
            summary["docs_differ"] += diffs
            summary["index_diffs"] += idx_diffs
            summary["schema_diffs"] += schema_diffs

            # collect samples
            for mid in (docs.get("missing_in_new") or []):
                samples["missing_new"].append((db_name, coll_name, mid))
            for mid in (docs.get("missing_in_old") or []):
                samples["missing_old"].append((db_name, coll_name, mid))
            for d in (docs.get("diffs") or []):
                diff_id = d[0] if isinstance(d, tuple) else d
                samples["doc_diffs"].append((db_name, coll_name, diff_id))
            for ix in (idx.get("missing_in_target") or []):
                samples["index_missing"].append((db_name, coll_name, ix))
            for ix in (idx.get("extra_in_target") or []):
                samples["index_extra"].append((db_name, coll_name, ix))
            for ix in (idx.get("changed") or []):
                samples["index_changed"].append((db_name, coll_name, ix))

            # per-collection recommendations
            if missing_new:
                per_collection_recs[(db_name, coll_name)].append(f"Backfill {missing_new} docs missing in NEW")
            if missing_old:
                per_collection_recs[(db_name, coll_name)].append(f"Investigate {missing_old} docs missing in OLD (possible deletes?)")
            if diffs:
                per_collection_recs[(db_name, coll_name)].append(f"Resolve {diffs} differing docs (consider higher sample% or full scan)")
            if idx_diffs:
                per_collection_recs[(db_name, coll_name)].append("Align index definitions (keys/unique/sparse)")
            if schema_diffs:
                per_collection_recs[(db_name, coll_name)].append("Normalize schema (field presence/types) before cutover")
    return summary, samples, per_collection_recs


def render_report(
    all_results: Dict,
    duration: float,
    use_color: bool,
    cdc_stats: Optional[List[Dict]] = None,
    report_top_n: int = 5,
    sample_percent: float = 100.0,
    anchor_sample: bool = False,
    explicit_ids_count: int = 0,
    latency_timestamp: Optional[str] = None,
):
    summary, samples, per_collection_recs = summarize_results(all_results)
    # group samples by collection for display
    coll_samples = defaultdict(lambda: defaultdict(list))
    for label, entries in samples.items():
        for (dbn, col, val) in entries:
            coll_samples[(dbn, col)][label].append(val)
    if use_color and Console and Table:
        console = Console()
        console.rule("[bold green]DocumentDB Consistency Report[/bold green]")

        sampling_mode = "anchored" if anchor_sample else "independent"
        summary_lines = [
            f"[green]Collections OK:[/green] {summary['collections_ok']}",
            f"[yellow]Collections with issues:[/yellow] {summary['collections_with_issues']}",
            f"[red]Collections missing:[/red] {summary['collections_missing']}",
            f"[red]Docs missing in NEW:[/red] {summary['docs_missing_new']}",
            f"[red]Docs missing in OLD:[/red] {summary['docs_missing_old']}",
            f"[magenta]Docs differing:[/magenta] {summary['docs_differ']}",
            f"[cyan]Index diffs:[/cyan] {summary['index_diffs']}",
            f"[cyan]Schema diffs:[/cyan] {summary['schema_diffs']}",
            f"[white]Sampling:[/white] ~{sample_percent:.2f}% per collection ({sampling_mode})",
            f"[white]Explicit IDs checked:[/white] {explicit_ids_count}",
            f"[white]Duration:[/white] {duration:.1f}s",
        ]
        if latency_timestamp:
            summary_lines.append(f"[white]Latency fetched at:[/white] {latency_timestamp}")
        console.print("\n".join(summary_lines))

        host_table = Table(
            title="Cluster endpoints",
            box=box.MINIMAL_DOUBLE_HEAD,
            header_style="bold cyan",
        )
        host_table.add_column("Role", style="magenta")
        host_table.add_column("Host", style="white")
        host_table.add_column("Port", style="white", justify="right")
        host_table.add_column("SSL", style="white", justify="center")
        host_table.add_row("OLD (3.6)", OLD_HOST, str(OLD_PORT), "yes" if OLD_SSL else "no")
        host_table.add_row("NEW (5.0)", NEW_HOST, str(NEW_PORT), "yes" if NEW_SSL else "no")
        console.print(host_table)

        if cdc_stats:
            lat_table = Table(
                title="CDC Latency (seconds)",
                box=box.SIMPLE,
                show_lines=False,
                header_style="bold magenta",
                padding=(0, 1),
            )
            lat_table.add_column("Task", style="cyan", no_wrap=True)
            lat_table.add_column("Instance", style="white", no_wrap=True)
            lat_table.add_column("Source", justify="right")
            lat_table.add_column("Target", justify="right")
            lat_table.add_column("Type", style="cyan", no_wrap=True)
            lat_table.add_column("Status", style="magenta", no_wrap=True)
            for stat in cdc_stats:
                src = stat.get("source_latency")
                tgt = stat.get("target_latency")
                src_str = f"{src:.1f}" if src is not None else "-"
                tgt_str = f"{tgt:.1f}" if tgt is not None else "-"
                lat_table.add_row(
                    stat.get("task_id", "-"),
                    stat.get("instance", "-"),
                    src_str,
                    tgt_str,
                    stat.get("migration_type", "-"),
                    stat.get("status", "-"),
                )
            console.print(lat_table)

        table = Table(
            title="Per-collection details",
            box=box.SIMPLE,
            show_lines=False,
            header_style="bold magenta",
            padding=(0, 1),
        )
        table.add_column("Database", style="cyan", no_wrap=True)
        table.add_column("Collection", style="cyan", no_wrap=True)
        table.add_column("Status", style="bold")
        table.add_column("MissingNew", justify="right")
        table.add_column("MissingOld", justify="right")
        table.add_column("OldTotal", justify="right")
        table.add_column("NewTotal", justify="right")
        table.add_column("Diffs", justify="right")
        table.add_column("IndexDiffs", justify="right")
        table.add_column("SchemaDiffs", justify="right")
        table.add_column("Duration(s)", justify="right")
        table.add_column("Sample%", justify="right")
        table.add_column("Notes", style="white")

        for db_name in sorted(all_results.keys()):
            for coll_name, result in sorted(all_results[db_name].items()):
                docs = result.get("documents", {})
                idx = result.get("indexes", {})
                schema = result.get("schema", {})

                missing_new = len(docs.get("missing_in_new", []) or [])
                missing_old = len(docs.get("missing_in_old", []) or [])
                diffs = len(docs.get("diffs", []) or [])
                idx_diffs = len(idx.get("missing_in_target", []) or []) + len(idx.get("extra_in_target", []) or []) + len(
                    idx.get("changed", []) or []
                )
                schema_diffs = len(schema.get("missing_in_new", []) or []) + len(schema.get("extra_in_new", []) or []) + len(
                    schema.get("type_mismatches", []) or []
                )
                sample_pct = docs.get("sample_percent", 0)
                duration_sec = result.get("duration_sec", 0.0)
                old_total = docs.get("old_total", 0)
                new_total = docs.get("new_total", 0)

                issues = result.get("issues", [])
                if issues and issues == ["No issues detected"]:
                    status_text = "[green]OK[/green]"
                elif "missing" in ";".join(issues).lower():
                    status_text = "[red]MISSING[/red]"
                else:
                    status_text = "[yellow]ISSUES[/yellow]"

                notes = "; ".join(issues)
                table.add_row(
                    db_name,
                    coll_name,
                    status_text,
                    str(missing_new),
                    str(missing_old),
                    str(old_total),
                    str(new_total),
                    str(diffs),
                    str(idx_diffs),
                    str(schema_diffs),
                    f"{duration_sec:.1f}",
                    f"{sample_pct}",
                    notes,
                )

        console.print(table)

        # Samples of missing IDs and index diffs (grouped per collection)
        if any(samples.values()):
            sample_table = Table(
                title=f"Top {report_top_n} samples per collection",
                box=box.MINIMAL,
                show_lines=False,
                header_style="bold magenta",
                padding=(0, 1),
            )
            sample_table.add_column("DB.Collection", style="cyan", no_wrap=True)
            sample_table.add_column("Type", style="white", no_wrap=True)
            sample_table.add_column("Details", style="white")

            def detail_for(val):
                if isinstance(val, dict) and "name" in val and "keys" in val:
                    keys_fmt = ",".join([f"{k}:{v}" for k, v in val.get("keys", [])])
                    return f"{val.get('name')} [{keys_fmt}]"
                return val

            for (dbn, col) in sorted(coll_samples.keys()):
                entries = coll_samples[(dbn, col)]
                for label, vals in entries.items():
                    for val in vals[:report_top_n]:
                        sample_table.add_row(f"{dbn}.{col}", label, str(detail_for(val)))

            console.print(sample_table)

        # Recommendations (global)
        recs = []
        if summary["docs_missing_new"] or summary["docs_missing_old"]:
            recs.append("Reconcile missing documents (check DMS filters, resume/reload, or backfill).")
        if summary["docs_differ"]:
            recs.append("Investigate differing docs; rerun with higher sample% or full scan; ensure application writers are paused.")
        if summary["index_diffs"]:
            recs.append("Align indexes (keys, unique, sparse) on target before cutover.")
        if summary["schema_diffs"]:
            recs.append("Normalize schema differences (field presence/types) to avoid runtime errors.")
        if cdc_stats:
            high_latency = [s for s in cdc_stats if (s.get("source_latency") or 0) > 5 or (s.get("target_latency") or 0) > 5]
            if high_latency:
                recs.append("Reduce CDC latency: scale DMS instance, tune task, or reduce target load.")
        if recs:
            rec_panel = "\n".join(f"- {r}" for r in recs)
            console.rule("[bold yellow]Recommendations[/bold yellow]")
            console.print(rec_panel)
            console.rule()

        # Recommendations per collection
        if per_collection_recs:
            rtable = Table(
                title="Per-collection recommendations",
                box=box.MINIMAL,
                show_lines=False,
                header_style="bold magenta",
                padding=(0, 1),
            )
            rtable.add_column("DB.Collection", style="cyan", no_wrap=True)
            rtable.add_column("Actions", style="white")
            for (dbn, col), rec_list in sorted(per_collection_recs.items()):
                rtable.add_row(f"{dbn}.{col}", "\n".join(f"- {r}" for r in rec_list))
            console.print(rtable)
    else:
        print("DocumentDB Consistency Report")
        print("-----------------------------")
        print(
            f"Collections OK: {summary['collections_ok']} | "
            f"With issues: {summary['collections_with_issues']} | "
            f"Missing: {summary['collections_missing']}"
        )
        print(
            f"Docs missing NEW: {summary['docs_missing_new']} | "
            f"Docs missing OLD: {summary['docs_missing_old']} | "
            f"Docs differing: {summary['docs_differ']}"
        )
        print(f"Index diffs: {summary['index_diffs']} | Schema diffs: {summary['schema_diffs']} | Duration: {duration:.1f}s")
        sampling_mode = "anchored" if anchor_sample else "independent"
        print(f"Sampling: ~{sample_percent:.2f}% per collection ({sampling_mode}); Explicit IDs checked: {explicit_ids_count}")
        if latency_timestamp:
            print(f"Latency fetched at: {latency_timestamp}")
        print(f"OLD host: {OLD_HOST}:{OLD_PORT} ssl={OLD_SSL} | NEW host: {NEW_HOST}:{NEW_PORT} ssl={NEW_SSL}")
        if cdc_stats:
            print("CDC Latency (seconds):")
            for stat in cdc_stats:
                src = stat.get("source_latency")
                tgt = stat.get("target_latency")
                print(
                    f" - {stat.get('task_id')} (instance {stat.get('instance')}): "
                    f"source={src if src is not None else '-'} target={tgt if tgt is not None else '-'} "
                    f"type={stat.get('migration_type')} status={stat.get('status')}"
                )
        print("")
        for db_name in sorted(all_results.keys()):
            for coll_name, result in sorted(all_results[db_name].items()):
                docs = result.get("documents", {})
                idx = result.get("indexes", {})
                schema = result.get("schema", {})

                missing_new = len(docs.get("missing_in_new", []) or [])
                missing_old = len(docs.get("missing_in_old", []) or [])
                diffs = len(docs.get("diffs", []) or [])
                idx_diffs = len(idx.get("missing_in_target", []) or []) + len(idx.get("extra_in_target", []) or []) + len(
                    idx.get("changed", []) or []
                )
                schema_diffs = len(schema.get("missing_in_new", []) or []) + len(schema.get("extra_in_new", []) or []) + len(
                    schema.get("type_mismatches", []) or []
                )
                sample_pct = docs.get("sample_percent", 0)
                duration_sec = result.get("duration_sec", 0.0)
                old_total = docs.get("old_total", 0)
                new_total = docs.get("new_total", 0)
                issues = "; ".join(result.get("issues", []))

                print(
                    f"{db_name}.{coll_name}: status={issues or 'ok'} "
                    f"missing_new={missing_new} missing_old={missing_old} "
                    f"old_total={old_total} new_total={new_total} "
                    f"diffs={diffs} index_diffs={idx_diffs} schema_diffs={schema_diffs} "
                    f"duration_s={duration_sec:.1f} sample%={sample_pct}"
                )
        print("")
        # Samples (plain, per collection)
        if any(samples.values()):
            print(f"Top {report_top_n} samples per collection:")
            def emit(label, entries):
                for (dbn, col, val) in entries[:report_top_n]:
                    if isinstance(val, dict) and "name" in val and "keys" in val:
                        keys_fmt = ",".join([f"{k}:{v}" for k, v in val.get("keys", [])])
                        val_fmt = f"{val.get('name')} [{keys_fmt}]"
                    else:
                        val_fmt = val
                    print(f" - {dbn}.{col} {label}: {val_fmt}")
            for (dbn, col) in sorted(coll_samples.keys()):
                entries = coll_samples[(dbn, col)]
                for label, vals in entries.items():
                    emit(label, [(dbn, col, v) for v in vals])
            print("")

        # Recommendations plain (global)
        recs = []
        if summary["docs_missing_new"] or summary["docs_missing_old"]:
            recs.append("Reconcile missing documents (check DMS filters, resume/reload, or backfill).")
        if summary["docs_differ"]:
            recs.append("Investigate differing docs; rerun with higher sample% or full scan; ensure application writers are paused.")
        if summary["index_diffs"]:
            recs.append("Align indexes (keys, unique, sparse) on target before cutover.")
        if summary["schema_diffs"]:
            recs.append("Normalize schema differences (field presence/types) to avoid runtime errors.")
        if cdc_stats:
            high_latency = [s for s in cdc_stats if (s.get("source_latency") or 0) > 5 or (s.get("target_latency") or 0) > 5]
            if high_latency:
                recs.append("Reduce CDC latency: scale DMS instance, tune task, or reduce target load.")
        if recs:
            print("Recommendations:")
            for r in recs:
                print(f" - {r}")
            print("")

        # Recommendations per collection (plain)
        if per_collection_recs:
            print("Per-collection recommendations:")
            for (dbn, col), rec_list in sorted(per_collection_recs.items()):
                for r in rec_list:
                    print(f" - {dbn}.{col}: {r}")
            print("")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two DocumentDB clusters for consistency.")
    parser.add_argument("--databases", nargs="*", help="Databases to compare (default: configured list).")
    parser.add_argument("--collections", nargs="*", help="Limit to specific collections.")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for fetching documents.")
    parser.add_argument("--diff-limit", type=int, default=50, help="Maximum differing documents to record per collection.")
    parser.add_argument(
        "--sample-size-percent",
        type=float,
        default=100.0,
        help="Percent of documents to sample for ID and schema checks (100 = full scan).",
    )
    parser.add_argument(
        "--sample-anchor-source",
        action="store_true",
        help="Sample IDs from source only and compare the same IDs on target (aligned sample).",
    )
    parser.add_argument(
        "--sampling-timeout-ms",
        type=int,
        default=500,
        help="Max time per aggregation/find when sampling.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars (tqdm). Progress auto-disables if tqdm is unavailable.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="Optional file to append findings (missing docs, diffs).",
    )
    parser.add_argument(
        "--no-latency",
        action="store_true",
        help="Skip CDC latency collection from DMS/CloudWatch.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable rich color report (auto-disabled if Rich not installed).",
    )
    parser.add_argument(
        "--report-top-n",
        type=int,
        default=5,
        help="How many sample items (missing ids, index diffs) to print in the report.",
    )
    parser.add_argument(
        "--check-ids",
        type=str,
        help="Comma-separated or JSON array of explicit _id values to check deterministically in every collection.",
    )
    parser.add_argument("--skip-deep-diff", action="store_true", help="Skip field-level deep diff to speed up runs.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    enable_deep_diff = not args.skip_deep_diff
    progress = not args.no_progress and tqdm is not None
    use_color = not args.no_color and Console is not None and Table is not None
    explicit_ids = parse_explicit_ids(args.check_ids)
    cdc_stats: Optional[List[Dict]] = None
    latency_timestamp: Optional[str] = None

    databases = args.databases or DEFAULT_DATABASES
    if not databases:
        print("No databases to compare. Pass --databases or set DBS.")
        return 1

    old_client = get_client(OLD_HOST, OLD_PORT, OLD_USER, OLD_PASS, OLD_SSL)
    new_client = get_client(NEW_HOST, NEW_PORT, NEW_USER, NEW_PASS, NEW_SSL)

    if not old_client:
        print("Failed to connect to OLD cluster; aborting.")
        return 1
    if not new_client:
        print("Warning: failed to connect to NEW cluster; comparisons limited to availability.")

    all_results = defaultdict(dict)
    start = datetime.now(timezone.utc)
    for db_name in databases:
        db_result = compare_database(
            db_name=db_name,
            old_client=old_client,
            new_client=new_client,
            batch_size=args.batch_size,
            diff_limit=args.diff_limit,
            collections=args.collections,
            enable_deep_diff=enable_deep_diff,
            sample_percent=args.sample_size_percent,
            timeout_ms=args.sampling_timeout_ms,
            progress=progress,
            output_file=args.output_file,
            explicit_ids=explicit_ids,
            anchor_sample=args.sample_anchor_source,
        )
        all_results[db_name] = db_result

    if not args.no_latency:
        latency_timestamp = datetime.now(timezone.utc).isoformat()
        cdc_stats = get_cdc_latency_data()

    duration = (datetime.now(timezone.utc) - start).total_seconds()
    render_report(
        all_results,
        duration,
        use_color,
        cdc_stats,
        report_top_n=args.report_top_n,
        sample_percent=args.sample_size_percent,
        anchor_sample=args.sample_anchor_source,
        explicit_ids_count=len(explicit_ids),
        latency_timestamp=latency_timestamp,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

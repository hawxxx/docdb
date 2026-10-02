#!/usr/bin/env python3
"""
DocumentDB Comparison Tool with Live Refresh
"""

import os
import pymongo
import ssl
import time
from datetime import datetime, timedelta, timezone

import boto3
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich import box
from rich.console import Group
from rich.live import Live

# Load environment variables from a .env file if present (do this FIRST!)
load_dotenv()

def _env_bool(name: str, default: bool = False) -> bool:
    """Read boolean environment variable"""
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "t", "yes", "y", "on")

# DocumentDB connection configuration (old / new)
OLD_HOST = os.getenv("OLD_HOST", "")
OLD_PORT = int(os.getenv("OLD_PORT", "27017"))
OLD_USER = os.getenv("OLD_USER", "")
OLD_PASS = os.getenv("OLD_PASS", "")
OLD_SSL = _env_bool("OLD_SSL", False)

NEW_HOST = os.getenv("NEW_HOST", "")
NEW_PORT = int(os.getenv("NEW_PORT", "27017"))
NEW_USER = os.getenv("NEW_USER", "")
NEW_PASS = os.getenv("NEW_PASS", "")
NEW_SSL = _env_bool("NEW_SSL", False)

DBS = os.getenv("DBS", "").split(",")
DBS = [db.strip() for db in DBS if db.strip()]
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "5"))

# AWS / DMS configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
# Optional: restrict to specific DMS tasks by identifier (leave empty to include all)
DMS_TASK_IDENTIFIERS = os.getenv("DMS_TASK_IDENTIFIERS", "").split(",")
DMS_TASK_IDENTIFIERS = [tid.strip() for tid in DMS_TASK_IDENTIFIERS if tid.strip()]

# Replication instance identifier used for CDC latency metrics (CloudWatch dimension)
# This should match the value you use in the AWS CLI, e.g.:
# aws cloudwatch get-metric-statistics --dimensions \
#   Name=ReplicationInstanceIdentifier,Value=<replication-instance-id> ...
DMS_REPLICATION_INSTANCE_IDENTIFIER = os.getenv("DMS_REPLICATION_INSTANCE_IDENTIFIER", "")

# SSL certificate path
SSL_CA_FILE = os.getenv("SSL_CA_FILE", "global-bundle.pem")

console = Console()

# Store previous values for trend tracking
previous_values = {}

# AWS clients (initialized lazily)
aws_session = None
dms_client = None
cloudwatch_client = None

# Internal flag to avoid spamming CDC debug logs
_cdc_debug_logged = False


def init_aws_clients():
    """Initialize boto3 clients if credentials are available"""
    global aws_session, dms_client, cloudwatch_client
    if dms_client and cloudwatch_client:
        return True
    try:
        aws_session = boto3.Session(region_name=AWS_REGION)
        dms_client = aws_session.client("dms")
        cloudwatch_client = aws_session.client("cloudwatch")
        return True
    except (BotoCoreError, ClientError) as exc:
        console.log(f"[red]AWS client init failed:[/red] {exc}")
        return False
    except Exception as exc:
        console.log(f"[red]AWS client init failed:[/red] {exc}")
        return False

def get_connection(host, port, user, password, ssl_enabled, db_name):
    """Create MongoDB connection"""
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
                serverSelectionTimeoutMS=5000
            )
        else:
            client = pymongo.MongoClient(
                f"mongodb://{user}:{password}@{host}:{port}/{db_name}",
                serverSelectionTimeoutMS=5000
            )
        # Test connection
        client.admin.command('ping')
        return client
    except Exception as e:
        return None


def extract_identifier_from_arn(arn):
    """Return the resource identifier portion of an ARN
    Examples:
    - arn:aws:dms:region:account:task:ID -> ID
    - arn:aws:dms:region:account:rep:INSTANCE_ID -> INSTANCE_ID
    - arn:aws:dms:region:account:task/ID -> ID
    """
    if not arn:
        return None
    # ARN format: arn:partition:service:region:account:resource
    parts = arn.split(":", 5)
    if len(parts) < 6:
        return arn
    resource = parts[5]
    # Resource can be "type:id" or "type/id", extract the ID part
    if ":" in resource:
        resource = resource.split(":", 1)[1]
    if "/" in resource:
        resource = resource.split("/", 1)[1]
    return resource


def normalize_task_identifier(value):
    """Accept either replication task IDs or full ARNs"""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("arn:"):
        return extract_identifier_from_arn(value)
    return value


def normalize_replication_instance(value):
    """Return replication instance identifier from either ID or ARN"""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.startswith("arn:"):
        value = extract_identifier_from_arn(value)
    # Replication instance IDs sometimes come prefixed with "rep:"
    if value.startswith("rep:"):
        value = value.split(":", 1)[1]
    return value

def get_collection_stats(client, db_name):
    """Get stats for all collections in a database"""
    if not client:
        return {}
    
    stats = {}
    try:
        db = client[db_name]
        for coll_name in db.list_collection_names():
            try:
                # Use collStats command - DocumentDB has opCounter and idxScans fields
                coll_stats = db.command("collStats", coll_name)
                
                # Get operation counters from opCounter object
                num_docs_ins = 0
                num_docs_upd = 0
                num_docs_del = 0
                
                if "opCounter" in coll_stats:
                    op_counter = coll_stats.get("opCounter", {})
                    num_docs_ins = int(op_counter.get("numDocsIns", 0) or 0)
                    num_docs_upd = int(op_counter.get("numDocsUpd", 0) or 0)
                    num_docs_del = int(op_counter.get("numDocsDel", 0) or 0)
                
                # Get collection scans (collScans) - direct field
                coll_scans = int(coll_stats.get("collScans", 0) or 0)
                
                # Get index scans (idxScans) - direct field in DocumentDB
                idx_scans = int(coll_stats.get("idxScans", 0) or 0)
                
                # Get total index size
                total_index_size = int(coll_stats.get("totalIndexSize", 0) or 0)
                
                stats[coll_name] = {
                    "count": int(coll_stats.get("count", 0) or 0),
                    "size": int(coll_stats.get("size", 0) or 0),
                    "storageSize": int(coll_stats.get("storageSize", 0) or 0),
                    "nindexes": int(coll_stats.get("nindexes", 0) or 0),
                    "totalIndexSize": total_index_size,
                    "collScans": coll_scans,
                    "idxScans": idx_scans,
                    "ops": f"{num_docs_ins}/{num_docs_upd}/{num_docs_del}",
                    "ops_ins": num_docs_ins,
                    "ops_upd": num_docs_upd,
                    "ops_del": num_docs_del
                }
            except Exception as e:
                stats[coll_name] = {"error": str(e)}
    except Exception as e:
        pass
    
    return stats


def fetch_cloudwatch_latency(instance_id, task_id, metric_name):
    """Fetch latency metric from CloudWatch when DMS stats are unavailable"""
    if not init_aws_clients():
        return None
    if not instance_id or not task_id:
        return None
    
    # Normalize both identifiers to ensure they match CloudWatch dimensions
    instance_id = normalize_replication_instance(instance_id)
    task_id = normalize_task_identifier(task_id)
    
    if not instance_id or not task_id:
        return None
    
    end_time = datetime.now(timezone.utc)
    # Look back 1 hour to match the working CLI example
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
    except (BotoCoreError, ClientError):
        return None
    except Exception:
        return None
    
    datapoints = response.get("Datapoints", [])
    if not datapoints:
        global _cdc_debug_logged
        if not _cdc_debug_logged:
            console.log(
                f"[yellow]CDC latency: no datapoints for instance_id={instance_id}, "
                f"task_id={task_id}, metric={metric_name}[/yellow]"
            )
            _cdc_debug_logged = True
        return None
    latest = max(datapoints, key=lambda x: x["Timestamp"])
    return latest.get("Average")

def format_number(num):
    """Format number with commas"""
    try:
        return f"{int(num):,}"
    except:
        return str(num)

def format_value_pair(old_fmt, old_trend, old_color, new_fmt, new_trend, new_color, old_width=22, new_width=22):
    """Format a value pair (Old|New) with consistent alignment
    old_width: width for old side (right-aligned)
    new_width: width for new side (left-aligned)
    """
    text = Text()
    
    # Old side: right-align (value + optional arrow)
    if old_trend:
        # Calculate how much padding we need before the value
        content_len = len(old_fmt) + 2  # value + space + arrow
        padding_len = max(0, old_width - content_len)
        text.append(" " * padding_len, style="white")
        text.append(old_fmt, style="white")
        text.append(f" {old_trend}", style=old_color)
    else:
        # No arrow, just right-align the value
        padding_len = max(0, old_width - len(old_fmt))
        text.append(" " * padding_len, style="white")
        text.append(old_fmt, style="white")
    
    text.append(" | ", style="cyan")
    
    # New side: left-align (value + optional arrow)
    if new_trend:
        text.append(new_fmt, style="white")
        text.append(f" {new_trend}", style=new_color)
        # Add remaining padding after
        content_len = len(new_fmt) + 2  # value + space + arrow
        padding_len = max(0, new_width - content_len)
        text.append(" " * padding_len, style="white")
    else:
        # No arrow, just left-align the value
        text.append(new_fmt, style="white")
        padding_len = max(0, new_width - len(new_fmt))
        text.append(" " * padding_len, style="white")
    
    return text

def format_header_pair(label, old_width, new_width):
    """Create a two-line header with aligned Old|New labels"""
    header_text = Text()
    total_width = old_width + new_width + 3  # include " | "
    centered_label = label.center(total_width)
    header_text.append(f"{centered_label}\n", style="bold magenta")
    header_text.append_text(
        format_value_pair(
            "Old",
            None,
            "white",
            "New",
            None,
            "white",
            old_width=old_width,
            new_width=new_width,
        )
    )
    return header_text

def get_trend_arrow(prev_val, curr_val, reverse=False):
    """Get trend arrow and color based on comparison
    → yellow for same, ↑ green for increase, ↓ red for decrease
    """
    try:
        prev_num = float(prev_val) if prev_val is not None else None
        curr_num = float(curr_val) if curr_val is not None else None
        
        # If no previous value, show no trend
        if prev_num is None:
            return "", "white"
        
        # If current value is missing/invalid
        if curr_num is None:
            return "❌", "red"
        
        if prev_num == curr_num:
            return "→", "yellow"
        elif curr_num > prev_num:
            # Increase
            if reverse:
                return "↓", "red"  # For metrics where lower is better
            return "↑", "green"
        else:
            # Decrease
            if reverse:
                return "↑", "green"  # For metrics where lower is better
            return "↓", "red"
    except Exception as e:
        return "", "white"

def get_trend_color(prev_val, curr_val, reverse=False):
    """Get color based on trend comparison
    yellow for same, green for increase, red for decrease
    """
    try:
        prev_num = float(prev_val) if prev_val is not None else None
        curr_num = float(curr_val) if curr_val is not None else None
        
        # If no previous value, show white
        if prev_num is None:
            return "white"
        
        # If current value is missing/invalid
        if curr_num is None:
            return "red"
        
        if prev_num == curr_num:
            return "yellow"
        elif curr_num > prev_num:
            # Increase
            if reverse:
                return "red"  # For metrics where lower is better
            return "green"
        else:
            # Decrease
            if reverse:
                return "green"  # For metrics where lower is better
            return "red"
    except Exception as e:
        return "white"

def format_ops_with_colors(
    old_ops_ins,
    old_ops_upd,
    old_ops_del,
    prev_ops_ins,
    prev_ops_upd,
    prev_ops_del,
    new_ops_str,
    prev_new_ops_ins=None,
    prev_new_ops_upd=None,
    prev_new_ops_del=None,
    old_width=42,
    new_width=42,
):
    """Format operations with colored numbers (no arrows) and aligned sides"""
    ops_text = Text()

    # Old side colors
    ins_color = get_trend_color(prev_ops_ins, old_ops_ins)
    upd_color = get_trend_color(prev_ops_upd, old_ops_upd)
    del_color = get_trend_color(prev_ops_del, old_ops_del)

    old_ins_fmt = format_number(old_ops_ins)
    old_upd_fmt = format_number(old_ops_upd)
    old_del_fmt = format_number(old_ops_del)
    old_str_len = len(old_ins_fmt) + len(old_upd_fmt) + len(old_del_fmt) + 2  # two slashes
    old_padding = max(0, old_width - old_str_len)
    if old_padding:
        ops_text.append(" " * old_padding, style="white")

    ops_text.append(old_ins_fmt, style=ins_color)
    ops_text.append("/", style="white")
    ops_text.append(old_upd_fmt, style=upd_color)
    ops_text.append("/", style="white")
    ops_text.append(old_del_fmt, style=del_color)
    ops_text.append(" | ", style="cyan")

    # New side colors (parse string if available)
    if new_ops_str != "-" and "/" in new_ops_str:
        try:
            # Handle both int and float strings (strip .0 if present)
            parts = new_ops_str.split("/", 2)
            new_ins = int(float(parts[0])) if parts[0] else 0
            new_upd = int(float(parts[1])) if len(parts) > 1 and parts[1] else 0
            new_del = int(float(parts[2])) if len(parts) > 2 and parts[2] else 0
            
            new_ins_color = (
                get_trend_color(prev_new_ops_ins, new_ins)
                if prev_new_ops_ins is not None
                else "white"
            )
            new_upd_color = (
                get_trend_color(prev_new_ops_upd, new_upd)
                if prev_new_ops_upd is not None
                else "white"
            )
            new_del_color = (
                get_trend_color(prev_new_ops_del, new_del)
                if prev_new_ops_del is not None
                else "white"
            )

            new_ins_fmt = format_number(new_ins)
            new_upd_fmt = format_number(new_upd)
            new_del_fmt = format_number(new_del)
            new_str_len = len(new_ins_fmt) + len(new_upd_fmt) + len(new_del_fmt) + 2

            ops_text.append(new_ins_fmt, style=new_ins_color)
            ops_text.append("/", style="white")
            ops_text.append(new_upd_fmt, style=new_upd_color)
            ops_text.append("/", style="white")
            ops_text.append(new_del_fmt, style=new_del_color)

            new_padding = max(0, new_width - new_str_len)
            if new_padding:
                ops_text.append(" " * new_padding, style="white")
        except (ValueError, IndexError):
            ops_text.append(new_ops_str, style="white")
            new_padding = max(0, new_width - len(new_ops_str))
            if new_padding:
                ops_text.append(" " * new_padding, style="white")
    else:
        display_str = new_ops_str if new_ops_str else "-"
        ops_text.append(display_str, style="white")
        new_padding = max(0, new_width - len(display_str))
        if new_padding:
            ops_text.append(" " * new_padding, style="white")

    return ops_text


def format_latency_value(current, previous):
    """Format CDC latency value with trend arrow (lower is better)"""
    if current is None:
        return Text("N/A", style="dim")
    # Latency is in seconds; show integer seconds when >= 1s, one decimal for sub-second (>0), and 0s for zero
    try:
        current_f = float(current)
    except Exception:
        current_f = 0.0
    if current_f == 0:
        value_fmt = "0s"
    elif current_f < 1.0:
        value_fmt = f"{current_f:.1f}s"
    else:
        value_fmt = f"{int(round(current_f))}s"
    trend_arrow, trend_color = get_trend_arrow(previous, current, reverse=True)
    text = Text(value_fmt, style=trend_color if trend_arrow else "white")
    if trend_arrow:
        text.append(f" {trend_arrow}", style=trend_color)
    return text


def get_cdc_latency_data():
    """Fetch CDC latency data for configured DMS tasks"""
    if not init_aws_clients():
        return []

    tasks = []
    try:
        # Fetch all tasks
        paginator = dms_client.get_paginator("describe_replication_tasks")
        for page in paginator.paginate():
            tasks.extend(page.get("ReplicationTasks", []))
        
        # Filter by configured identifiers if specified
        if DMS_TASK_IDENTIFIERS:
            normalized_ids = set(normalize_task_identifier(tid) for tid in DMS_TASK_IDENTIFIERS if tid)
            tasks = [
                task
                for task in tasks
                if task.get("ReplicationTaskIdentifier") in normalized_ids
            ]
    except Exception:
        # Silently fail - no console output during refresh
        return []

    latencies = []
    for task in tasks:
        task_identifier = task.get("ReplicationTaskIdentifier")
        task_arn = task.get("ReplicationTaskArn")
        task_status = task.get("Status", "unknown")
        migration_type = task.get("MigrationType", "-")
        # We rely on CloudWatch for latency, since DescribeReplicationTasks often returns None
        stats = task.get("ReplicationTaskStats") or {}
        source_latency = stats.get("SourceLatency")
        target_latency = stats.get("TargetLatency")

        # Use configured replication instance identifier for CloudWatch (matches CLI example)
        if DMS_REPLICATION_INSTANCE_IDENTIFIER:
            instance_id = DMS_REPLICATION_INSTANCE_IDENTIFIER
        else:
            # Fallback: extract from ARN if config not set
            instance_arn = task.get("ReplicationInstanceArn")
            instance_id = normalize_replication_instance(instance_arn) if instance_arn else None
        
        # For CloudWatch, we need the short task ID from the ARN (e.g., U4JBX746VZFSHBDQCZHCKTVGNI)
        # The ReplicationTaskIdentifier is the friendly name, but CloudWatch uses the short ID from ARN
        task_id_for_cw = None
        if task_arn:
            # Extract the short ID from ARN: arn:aws:dms:region:account:task:SHORT_ID
            task_id_for_cw = extract_identifier_from_arn(task_arn)
        elif task_identifier and len(task_identifier) > 20:
            # If no ARN but identifier looks like a short ID (long alphanumeric), use it
            task_id_for_cw = task_identifier
        else:
            # Try to find task by identifier and get its ARN
            task_id_for_cw = task_identifier

        # Always query CloudWatch for latency, regardless of DMS stats
        if instance_id and task_id_for_cw:
            source_latency = fetch_cloudwatch_latency(
                instance_id, task_id_for_cw, "CDCLatencySource"
            )
            target_latency = fetch_cloudwatch_latency(
                instance_id, task_id_for_cw, "CDCLatencyTarget"
            )

        latencies.append(
            {
                "task_id": task_identifier,
                "status": task_status,
                "migration_type": migration_type,
                "source_latency": source_latency,
                "target_latency": target_latency,
            }
        )

    return latencies

def create_display(old_client, new_client):
    """Create the complete display"""
    global previous_values
    
    # Header
    header = Panel(
        f"[bold green]DocumentDB Comparison Tool[/bold green]\n"
        f"[dim]Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
        box=box.ROUNDED,
        border_style="green"
    )
    
    # Create tables for each database
    tables = []
    current_values = {}  # Store current values for next iteration

    cdc_stats = get_cdc_latency_data()
    cdc_table = None
    if cdc_stats:
        cdc_table = Table(
            title="[bold cyan]CDC Latency (seconds)[/bold cyan]",
            show_header=True,
            header_style="bold magenta",
            box=box.SIMPLE,
            padding=(0, 1),
        )
        cdc_table.add_column("Task", style="cyan", no_wrap=True)
        cdc_table.add_column("Source", justify="right")
        cdc_table.add_column("Target", justify="right")
        cdc_table.add_column("MigrationType", style="cyan", no_wrap=True)
        cdc_table.add_column("Status", style="magenta", no_wrap=True)

        for stat in cdc_stats:
            task_key = f"dms.{stat['task_id']}"
            prev_latency = previous_values.get(task_key, {})
            src_text = format_latency_value(stat["source_latency"], prev_latency.get("source_latency"))
            tgt_text = format_latency_value(stat["target_latency"], prev_latency.get("target_latency"))

            current_values[task_key] = {
                "source_latency": stat["source_latency"],
                "target_latency": stat["target_latency"],
            }

            cdc_table.add_row(
                stat["task_id"],
                src_text,
                tgt_text,
                stat["migration_type"],
                stat["status"],
            )
    
    for db_name in DBS:
        old_stats = get_collection_stats(old_client, db_name) if old_client else {}
        new_stats = get_collection_stats(new_client, db_name) if new_client else {}
        
        table = Table(
            title=f"[bold cyan]{db_name}[/bold cyan]",
            show_header=True,
            box=box.SIMPLE,
            padding=(0, 1),
            show_lines=False
        )
        
        # Add columns with increased widths to prevent wrapping and truncation
        # Each side can have up to 15 chars for number + 2 for arrow = 17 chars per side
        # Total per column: 17 + 3 (" | ") + 17 = 37 chars minimum
        table.add_column("Collection", style="cyan", width=35, no_wrap=False)
        table.add_column(format_header_pair("Count", 23, 23), justify="left", width=50, no_wrap=True)
        table.add_column(format_header_pair("Size", 24, 24), justify="left", width=52, no_wrap=True)
        table.add_column(format_header_pair("Storage", 24, 24), justify="left", width=52, no_wrap=True)
        table.add_column(format_header_pair("nindexes", 8, 8), justify="left", width=20, no_wrap=True)
        table.add_column(format_header_pair("IdxSize", 24, 24), justify="left", width=52, no_wrap=True)
        table.add_column(format_header_pair("CollScans", 14, 14), justify="left", width=32, no_wrap=True)
        table.add_column(format_header_pair("IdxScans", 16, 16), justify="left", width=36, no_wrap=True)
        table.add_column(format_header_pair("Ops I/U/D", 42, 42), justify="left", width=90, no_wrap=True)
        
        # Get all collection names
        all_collections = set(old_stats.keys()) | set(new_stats.keys())
        
        for coll_name in sorted(all_collections):
            old = old_stats.get(coll_name, {})
            new = new_stats.get(coll_name, {})
            
            if "error" in old:
                continue
            
            # Get current values
            old_count = old.get("count", 0)
            new_count = new.get("count", 0) if coll_name in new_stats else None
            old_size = old.get("size", 0)
            new_size = new.get("size", 0) if coll_name in new_stats else None
            old_storage = old.get("storageSize", 0)
            new_storage = new.get("storageSize", 0) if coll_name in new_stats else None
            old_nindexes = old.get("nindexes", 0)
            new_nindexes = new.get("nindexes", 0) if coll_name in new_stats else None
            old_idxsz = old.get("totalIndexSize", 0)
            new_idxsz = new.get("totalIndexSize", 0) if coll_name in new_stats else None
            old_coll_scans = old.get("collScans", 0)
            new_coll_scans = new.get("collScans", 0) if coll_name in new_stats else None
            old_idx_scans = old.get("idxScans", 0)
            new_idx_scans = new.get("idxScans", 0) if coll_name in new_stats else None
            
            # Get ops values
            old_ops_ins = old.get("ops_ins", 0)
            old_ops_upd = old.get("ops_upd", 0)
            old_ops_del = old.get("ops_del", 0)
            new_ops_str = "-" if coll_name not in new_stats else new.get("ops", "0/0/0")
            new_ops_ins = new.get("ops_ins", 0) if coll_name in new_stats else None
            new_ops_upd = new.get("ops_upd", 0) if coll_name in new_stats else None
            new_ops_del = new.get("ops_del", 0) if coll_name in new_stats else None
            
            # Store current values for next iteration
            key = f"{db_name}.{coll_name}"
            current_values[key] = {
                "old_count": old_count,
                "new_count": new_count,
                "old_size": old_size,
                "new_size": new_size,
                "old_storage": old_storage,
                "new_storage": new_storage,
                "old_nindexes": old_nindexes,
                "new_nindexes": new_nindexes,
                "old_idxsz": old_idxsz,
                "new_idxsz": new_idxsz,
                "old_coll_scans": old_coll_scans,
                "new_coll_scans": new_coll_scans,
                "old_idx_scans": old_idx_scans,
                "new_idx_scans": new_idx_scans,
                "old_ops_ins": old_ops_ins,
                "old_ops_upd": old_ops_upd,
                "old_ops_del": old_ops_del,
                "new_ops_ins": new_ops_ins,
                "new_ops_upd": new_ops_upd,
                "new_ops_del": new_ops_del
            }
            
            # Get previous values
            prev = previous_values.get(key, {})
            
            # Format values and get trends (compare current to previous)
            old_count_fmt = format_number(old_count)
            new_count_fmt = "MISSING" if new_count is None else format_number(new_count)
            old_count_trend, old_count_color = get_trend_arrow(prev.get("old_count"), old_count)
            new_count_trend, new_count_color = get_trend_arrow(prev.get("new_count"), new_count)
            count_text = format_value_pair(
                old_count_fmt,
                old_count_trend,
                old_count_color,
                new_count_fmt,
                new_count_trend,
                new_count_color,
                old_width=23,
                new_width=23,
            )
            
            old_size_fmt = format_number(old_size)
            new_size_fmt = "-" if new_size is None else format_number(new_size)
            old_size_trend, old_size_color = get_trend_arrow(prev.get("old_size"), old_size)
            new_size_trend, new_size_color = get_trend_arrow(prev.get("new_size"), new_size)
            size_text = format_value_pair(
                old_size_fmt,
                old_size_trend,
                old_size_color,
                new_size_fmt,
                new_size_trend,
                new_size_color,
                old_width=24,
                new_width=24,
            )
            
            old_storage_fmt = format_number(old_storage)
            new_storage_fmt = "-" if new_storage is None else format_number(new_storage)
            old_storage_trend, old_storage_color = get_trend_arrow(prev.get("old_storage"), old_storage)
            new_storage_trend, new_storage_color = get_trend_arrow(prev.get("new_storage"), new_storage)
            storage_text = format_value_pair(
                old_storage_fmt,
                old_storage_trend,
                old_storage_color,
                new_storage_fmt,
                new_storage_trend,
                new_storage_color,
                old_width=24,
                new_width=24,
            )
            
            # nindexes formatting
            old_nindexes_fmt = str(old_nindexes)
            new_nindexes_fmt = "-" if new_nindexes is None else str(new_nindexes)
            old_nindexes_trend, old_nindexes_color = get_trend_arrow(prev.get("old_nindexes"), old_nindexes)
            new_nindexes_trend, new_nindexes_color = get_trend_arrow(prev.get("new_nindexes"), new_nindexes)
            nindexes_text = format_value_pair(
                old_nindexes_fmt,
                old_nindexes_trend,
                old_nindexes_color,
                new_nindexes_fmt,
                new_nindexes_trend,
                new_nindexes_color,
                old_width=8,
                new_width=8,
            )
            
            old_idxsz_fmt = format_number(old_idxsz)
            new_idxsz_fmt = "-" if new_idxsz is None else format_number(new_idxsz)
            old_idxsz_trend, old_idxsz_color = get_trend_arrow(prev.get("old_idxsz"), old_idxsz)
            new_idxsz_trend, new_idxsz_color = get_trend_arrow(prev.get("new_idxsz"), new_idxsz)
            idxsz_text = format_value_pair(
                old_idxsz_fmt,
                old_idxsz_trend,
                old_idxsz_color,
                new_idxsz_fmt,
                new_idxsz_trend,
                new_idxsz_color,
                old_width=24,
                new_width=24,
            )
            
            # CollScans with trends (normal trends: increase=green, decrease=red)
            old_coll_scans_fmt = format_number(old_coll_scans)
            new_coll_scans_fmt = "-" if new_coll_scans is None else format_number(new_coll_scans)
            old_coll_scans_trend, old_coll_scans_color = get_trend_arrow(prev.get("old_coll_scans"), old_coll_scans)
            new_coll_scans_trend, new_coll_scans_color = get_trend_arrow(prev.get("new_coll_scans"), new_coll_scans)
            coll_scans_text = format_value_pair(
                old_coll_scans_fmt,
                old_coll_scans_trend,
                old_coll_scans_color,
                new_coll_scans_fmt,
                new_coll_scans_trend,
                new_coll_scans_color,
                old_width=14,
                new_width=14,
            )
            
            # IdxScans with trends (normal trends: increase=green, decrease=red)
            old_idx_scans_fmt = format_number(old_idx_scans)
            new_idx_scans_fmt = "-" if new_idx_scans is None else format_number(new_idx_scans)
            old_idx_scans_trend, old_idx_scans_color = get_trend_arrow(prev.get("old_idx_scans"), old_idx_scans)
            new_idx_scans_trend, new_idx_scans_color = get_trend_arrow(prev.get("new_idx_scans"), new_idx_scans)
            idx_scans_text = format_value_pair(
                old_idx_scans_fmt,
                old_idx_scans_trend,
                old_idx_scans_color,
                new_idx_scans_fmt,
                new_idx_scans_trend,
                new_idx_scans_color,
                old_width=16,
                new_width=16,
            )
            
            # Format ops with colored numbers (no arrows)
            ops_text = format_ops_with_colors(
                old_ops_ins,
                old_ops_upd,
                old_ops_del,
                prev.get("old_ops_ins"),
                prev.get("old_ops_upd"),
                prev.get("old_ops_del"),
                new_ops_str,
                prev.get("new_ops_ins"),
                prev.get("new_ops_upd"),
                prev.get("new_ops_del"),
                old_width=42,
                new_width=42,
            )
            
            table.add_row(
                coll_name,
                count_text,
                size_text,
                storage_text,
                nindexes_text,
                idxsz_text,
                coll_scans_text,
                idx_scans_text,
                ops_text
            )
        
        tables.append(table)
    
    # Update previous values for next iteration
    previous_values = current_values
    
    # Footer
    footer = Panel(
        f"[dim]Refreshing every {REFRESH_INTERVAL} seconds | Press Ctrl+C to exit[/dim]",
        box=box.ROUNDED,
        border_style="dim"
    )
    
    # Combine everything using Group - no extra blank lines between tables
    renderables = [header]
    if cdc_table:
        renderables.append(cdc_table)
        renderables.append("")
    for i, table in enumerate(tables):
        renderables.append(table)
        # Only add blank line if not the last table
        if i < len(tables) - 1:
            renderables.append("")
    renderables.append(footer)
    
    return Group(*renderables)

def main():
    """Main function with live refresh"""
    console.clear()
    
    # Connect to databases
    console.print("[yellow]Connecting to databases...[/yellow]")
    old_client = get_connection(OLD_HOST, OLD_PORT, OLD_USER, OLD_PASS, OLD_SSL, "")
    new_client = get_connection(NEW_HOST, NEW_PORT, NEW_USER, NEW_PASS, NEW_SSL, "")
    
    if not old_client:
        console.print("[red]Failed to connect to OLD database[/red]")
        return
    
    if not new_client:
        console.print("[yellow]Warning: Failed to connect to NEW database[/yellow]")
    
    # Use Live with screen=True for smoothest possible updates
    try:
        with Live(
            console=console,
            screen=True,
            auto_refresh=False,
            transient=False
        ) as live:
            while True:
                display = create_display(old_client, new_client)
                live.update(display)
                live.refresh()
                time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        console.print("\n[yellow]Exiting...[/yellow]")
    finally:
        if old_client:
            old_client.close()
        if new_client:
            new_client.close()

if __name__ == "__main__":
    main()

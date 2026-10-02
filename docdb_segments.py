# Modified from dms-segments.py in amazon-documentdb-tools
# (https://github.com/awslabs/amazon-documentdb-tools), licensed under the
# Apache License, Version 2.0. Changes: checkpoint/resume support, retry with
# exponential backoff, configurable connection timeouts, progress output,
# throttled parallel scan mode (--parallel).

from datetime import datetime, timedelta, timezone
import sys
import json
import pymongo
import time
import os
import argparse
import warnings
import bisect
import threading
import concurrent.futures
from bson import ObjectId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure, NetworkTimeout


supportedIdTypes=['int','string','objectId']

def save_checkpoint(checkpoint_file, checkpoint_data):
    """Save checkpoint data to file"""
    try:
        # Convert ObjectIds to strings for JSON serialization
        checkpoint_json = {
            'boundaries': [str(b) for b in checkpoint_data.get('boundaries', [])],
            'numDocsTotal': checkpoint_data.get('numDocsTotal', 0),
            'numDocsBoundary': checkpoint_data.get('numDocsBoundary', 0),
            'thisBoundary': checkpoint_data.get('thisBoundary', 0),
            'startFromId': str(checkpoint_data.get('startFromId')) if checkpoint_data.get('startFromId') else None,
            'lastId': checkpoint_data.get('lastId', checkpoint_data.get('startFromId', None)),  # Current _id being processed
            'queryStartTime': checkpoint_data.get('queryStartTime', time.time()),
            'numDocuments': checkpoint_data.get('numDocuments', 0),
            'feedbackDocuments': checkpoint_data.get('feedbackDocuments', 0),
            'progressDocuments': checkpoint_data.get('progressDocuments', 0),
            'database': checkpoint_data.get('database', ''),
            'collection': checkpoint_data.get('collection', ''),
            'numSegments': checkpoint_data.get('numSegments', 0),
            'lastUpdateTime': time.time()
        }
        # Convert lastId to string if it's an ObjectId
        if checkpoint_json['lastId'] and not isinstance(checkpoint_json['lastId'], str):
            checkpoint_json['lastId'] = str(checkpoint_json['lastId'])
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_json, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save checkpoint: {e}")

def load_checkpoint(checkpoint_file):
    """Load checkpoint data from file"""
    try:
        if not os.path.exists(checkpoint_file):
            return None
        
        with open(checkpoint_file, 'r') as f:
            checkpoint_json = json.load(f)
        
        # Convert string ObjectIds back to ObjectId objects
        startFromId_str = checkpoint_json.get('startFromId', None)
        lastId_str = checkpoint_json.get('lastId', startFromId_str)  # Fallback to startFromId if lastId missing
        
        checkpoint_data = {
            'boundaries': [ObjectId(b) for b in checkpoint_json.get('boundaries', [])],
            'numDocsTotal': checkpoint_json.get('numDocsTotal', 0),
            'numDocsBoundary': checkpoint_json.get('numDocsBoundary', 0),
            'thisBoundary': checkpoint_json.get('thisBoundary', 0),
            'startFromId': ObjectId(startFromId_str) if startFromId_str else None,
            'lastId': str(lastId_str) if lastId_str else None,  # Keep as string for display, handle old checkpoints
            'queryStartTime': checkpoint_json.get('queryStartTime', time.time()),
            'numDocuments': checkpoint_json.get('numDocuments', 0),
            'feedbackDocuments': checkpoint_json.get('feedbackDocuments', 0),
            'progressDocuments': checkpoint_json.get('progressDocuments', 0),
            'database': checkpoint_json.get('database', ''),
            'collection': checkpoint_json.get('collection', ''),
            'numSegments': checkpoint_json.get('numSegments', 0),
            'lastUpdateTime': checkpoint_json.get('lastUpdateTime', time.time())
        }
        return checkpoint_data
    except Exception as e:
        print(f"Warning: Failed to load checkpoint: {e}")
        return None

def retry_with_backoff(max_retries=5, initial_delay=1, backoff_factor=2, exceptions=(ConnectionFailure, ServerSelectionTimeoutError, NetworkTimeout, OperationFailure)):
    """Decorator to retry a function with exponential backoff"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"  Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"  All {max_retries} attempts failed. Last error: {e}")
            raise last_exception
        return wrapper
    return decorator


def via_skips(appConfig):
    # get boundaries by performing large server-side skips
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    boundaryList = []

    numBoundaries = appConfig['numSegments'] - 1

    connectTimeoutMS = appConfig.get('connectTimeoutMS', 30000)
    serverSelectionTimeoutMS = appConfig.get('serverSelectionTimeoutMS', 30000)
    socketTimeoutMS = appConfig.get('socketTimeoutMS', 300000)

    @retry_with_backoff(max_retries=5, initial_delay=2, backoff_factor=2)
    def get_client():
        return pymongo.MongoClient(
            host=appConfig['uri'],
            appname='segmentr',
            connectTimeoutMS=connectTimeoutMS,
            serverSelectionTimeoutMS=serverSelectionTimeoutMS,
            socketTimeoutMS=socketTimeoutMS
        )

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
    def get_coll_stats(db):
        return db.command("collStats", appConfig['collection'])

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
    def find_one_with_retry(col, filter, projection, sort, skip=None):
        if skip is not None:
            return col.find_one(filter=filter, projection=projection, sort=sort, skip=skip)
        else:
            return col.find_one(filter=filter, projection=projection, sort=sort)

    client = get_client()
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = get_coll_stats(db)
    numDocuments = collStats['count']
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])
    progressDocuments = int((numDocuments - feedbackDocuments)*0.01)

    print("")
    print("collection {}.{} contains {} documents".format(appConfig['database'],appConfig['collection'],numDocuments))
    print("finding _id values for {} chunks, approximately {} documents in each".format(appConfig['numSegments'],feedbackDocuments))

    queryStartTime = time.time()

    # get the first _id
    currentId = find_one_with_retry(col, filter=None, projection={"_id":True}, sort=[("_id",pymongo.ASCENDING)])
    print("  found first _id")
    numDocsTotal = 0

    for x in range(numBoundaries):
        currentId = find_one_with_retry(
            col,
            filter={"_id":{"$gt":currentId["_id"]}},
            projection={"_id":True},
            sort=[("_id",pymongo.ASCENDING)],
            skip=feedbackDocuments
        )
        numDocsTotal += feedbackDocuments
        pctDone = numDocsTotal/(numDocuments - feedbackDocuments)*100
        elapsedSecs = int(time.time() - queryStartTime)
        estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs)
        print("  boundary {:3d} - {} {} | done in approximately {} seconds".format(x+1,type(currentId["_id"]),currentId["_id"],estimatedSecsToDone))
        boundaryList.append(currentId["_id"])

    boundaryListAsString = "{}".format(",".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list | {}".format(boundaryListAsString))

    boundaryListAsStringForDms = "[{}]".format("],[".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list for DMS | {}".format(boundaryListAsStringForDms))

    print("")

    queryElapsedSecs = int(time.time() - queryStartTime)
    print('query required {} seconds'.format(queryElapsedSecs))

    print("")
        
    client.close()


def via_cursor(appConfig):
    # get by walking the _id index

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    numBoundaries = appConfig['numSegments'] - 1
    boundaryList = []
    resumeFromBoundary = appConfig.get('resumeFromBoundaries', [])
    checkpoint_file = appConfig.get('checkpointFile', None)
    
    # Connection timeout settings (in milliseconds)
    connectTimeoutMS = appConfig.get('connectTimeoutMS', 30000)  # 30 seconds
    serverSelectionTimeoutMS = appConfig.get('serverSelectionTimeoutMS', 30000)  # 30 seconds
    socketTimeoutMS = appConfig.get('socketTimeoutMS', 300000)  # 5 minutes

    @retry_with_backoff(max_retries=5, initial_delay=2, backoff_factor=2)
    def get_client():
        return pymongo.MongoClient(
            host=appConfig['uri'],
            appname='segmentr',
            connectTimeoutMS=connectTimeoutMS,
            serverSelectionTimeoutMS=serverSelectionTimeoutMS,
            socketTimeoutMS=socketTimeoutMS
        )

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
    def get_coll_stats(client, db):
        return db.command("collStats", appConfig['collection'])

    client = get_client()
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = get_coll_stats(client, db)
    numDocuments = collStats['count']
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])
    progressDocuments = int((numDocuments - feedbackDocuments)*0.01)

    print("")
    print("collection {}.{} contains {} documents".format(appConfig['database'],appConfig['collection'],numDocuments))
    print("finding _id values for {} chunks, approximately {} documents in each".format(appConfig['numSegments'],feedbackDocuments))

    # Try to load checkpoint first
    checkpoint_data = None
    if checkpoint_file:
        checkpoint_data = load_checkpoint(checkpoint_file)
        if checkpoint_data:
            # Verify checkpoint matches current run
            if (checkpoint_data['database'] == appConfig['database'] and 
                checkpoint_data['collection'] == appConfig['collection'] and
                checkpoint_data['numSegments'] == appConfig['numSegments']):
                print("")
                print("Checkpoint found and loaded:")
                print("  boundaries found: {}".format(len(checkpoint_data['boundaries'])))
                print("  documents processed: {:,}".format(checkpoint_data['numDocsTotal']))
                print("  last boundary: {}".format(checkpoint_data['boundaries'][-1] if checkpoint_data['boundaries'] else "none"))
                if checkpoint_data.get('lastId'):
                    print("  last processed _id: {}".format(checkpoint_data['lastId']))
                print("")
                boundaryList = checkpoint_data['boundaries'].copy()
                numDocsTotal = checkpoint_data['numDocsTotal']
                # Reset numDocsBoundary to 0 since we're starting from after the last boundary
                numDocsBoundary = 0
                thisBoundary = checkpoint_data['thisBoundary']
                startFromId = checkpoint_data['startFromId']
                # Reset queryStartTime to current time for accurate elapsed time calculation
                queryStartTime = time.time()
                
                # Show boundaries in the same format as original output
                if boundaryList:
                    print("Resuming from checkpoint. Boundaries found so far:")
                    boundaryNum = 0
                    for boundary_id in boundaryList:
                        boundaryNum += 1
                        print("  boundary {:3d} - objectid {}".format(boundaryNum, boundary_id))
                    print("")
                    print("Resuming from boundary {}, starting after objectid {}".format(thisBoundary, startFromId))
                    if checkpoint_data.get('lastId'):
                        print("Last processed _id in checkpoint: {}".format(checkpoint_data['lastId']))
                    print("Approximate starting position: {:,} documents".format(numDocsTotal))
                    print("")
            else:
                print("")
                print("Checkpoint found but doesn't match current parameters. Starting fresh.")
                checkpoint_data = None

    # Handle resume from boundaries (if no checkpoint or checkpoint invalid)
    startFromId = None
    numDocsTotal = 0
    numDocsBoundary = 0
    thisBoundary = 0
    
    if not checkpoint_data:
        if resumeFromBoundary:
            print("")
            print("Resuming from {} existing boundaries:".format(len(resumeFromBoundary)))
            for idx, boundary_id in enumerate(resumeFromBoundary, 1):
                print("  boundary {:3d} - objectid {}".format(idx, boundary_id))
                boundaryList.append(boundary_id)
            
            thisBoundary = len(resumeFromBoundary)
            startFromId = resumeFromBoundary[-1]
            # Calculate approximate starting position
            numDocsTotal = thisBoundary * feedbackDocuments
            numDocsBoundary = 0  # Reset for next boundary
            print("")
            print("Resuming from boundary {}, starting after objectid {}".format(thisBoundary, startFromId))
            print("Approximate starting position: {} documents".format(numDocsTotal))
        
        queryStartTime = time.time()
    
    # Create cursor with or without resume filter
    if startFromId:
        @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
        def create_cursor():
            return col.find(
                filter={"_id": {"$gt": startFromId}},
                projection={"_id": True},
                sort=[("_id", pymongo.ASCENDING)]
            )
        cursor = create_cursor()
    else:
        @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
        def create_cursor():
            return col.find(
                filter=None,
                projection={"_id": True},
                sort=[("_id", pymongo.ASCENDING)]
            )
        cursor = create_cursor()
    
    print("..cursor created")
    print("Starting to process documents...")
    
    # Track last checkpoint save time to avoid saving too frequently
    last_checkpoint_save = time.time()
    checkpoint_save_interval = 60  # Save checkpoint every 60 seconds
    
    # Track for immediate feedback
    last_feedback_time = time.time()
    feedback_interval = 10  # Show feedback every 10 seconds
    docs_since_last_feedback = 0
    lastProcessedId = None  # Track last processed _id
    
    try:
        for thisDoc in cursor:
            numDocsTotal += 1
            numDocsBoundary += 1
            docs_since_last_feedback += 1
            lastProcessedId = thisDoc["_id"]  # Track current _id
            
            # Show immediate feedback every N seconds to confirm it's working
            current_time = time.time()
            if (current_time - last_feedback_time) >= feedback_interval:
                rate = docs_since_last_feedback / (current_time - last_feedback_time)
                elapsedSecs = int(current_time - queryStartTime)
                pctDone = numDocsTotal / (numDocuments - feedbackDocuments) * 100 if (numDocuments - feedbackDocuments) > 0 else 0
                estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs) if pctDone > 0 else 0
                
                # Format elapsed time
                elapsedHours = elapsedSecs // 3600
                elapsedMins = (elapsedSecs % 3600) // 60
                elapsedSecsRem = elapsedSecs % 60
                elapsedStr = "{:d}h{:02d}m{:02d}s".format(elapsedHours, elapsedMins, elapsedSecsRem) if elapsedHours > 0 else "{:d}m{:02d}s".format(elapsedMins, elapsedSecsRem)
                
                # Format estimated time remaining
                estHours = estimatedSecsToDone // 3600
                estMins = (estimatedSecsToDone % 3600) // 60
                estSecsRem = estimatedSecsToDone % 60
                estStr = "{:d}h{:02d}m{:02d}s".format(estHours, estMins, estSecsRem) if estHours > 0 else "{:d}m{:02d}s".format(estMins, estSecsRem)
                
                # Improved single-line format
                boundaryPct = (numDocsBoundary/feedbackDocuments*100) if feedbackDocuments > 0 else 0
                print("  docs={:,} | pct={:.1f}% | elapsed={} | remaining={} | rate={:.0f} docs/sec | boundary={:,}/{:,} ({:.1f}%) | lastId={}".format(
                    numDocsTotal, pctDone, elapsedStr, estStr, rate, numDocsBoundary, feedbackDocuments, boundaryPct, thisDoc["_id"]))
                
                docs_since_last_feedback = 0
                last_feedback_time = current_time
                
                # Save checkpoint periodically (every checkpoint_save_interval seconds)
                if checkpoint_file and (current_time - last_checkpoint_save) >= checkpoint_save_interval:
                    checkpoint_data = {
                        'boundaries': boundaryList,
                        'numDocsTotal': numDocsTotal,
                        'numDocsBoundary': numDocsBoundary,
                        'thisBoundary': thisBoundary,
                        'startFromId': thisDoc["_id"],  # Current _id being processed
                        'lastId': str(thisDoc["_id"]),  # Also save as lastId for reference
                        'queryStartTime': queryStartTime,
                        'numDocuments': numDocuments,
                        'feedbackDocuments': feedbackDocuments,
                        'progressDocuments': progressDocuments,
                        'database': appConfig['database'],
                        'collection': appConfig['collection'],
                        'numSegments': appConfig['numSegments']
                    }
                    save_checkpoint(checkpoint_file, checkpoint_data)
                    last_checkpoint_save = current_time

            if (numDocsBoundary >= feedbackDocuments):
                numDocsBoundary = 0
                thisBoundary += 1
                print("  boundary {:3d} - objectid {}".format(thisBoundary,thisDoc["_id"]))
                boundaryList.append(thisDoc["_id"])
                
                # Save checkpoint after each boundary
                if checkpoint_file:
                    checkpoint_data = {
                        'boundaries': boundaryList,
                        'numDocsTotal': numDocsTotal,
                        'numDocsBoundary': numDocsBoundary,
                        'thisBoundary': thisBoundary,
                        'startFromId': thisDoc["_id"],
                        'lastId': str(thisDoc["_id"]),
                        'queryStartTime': queryStartTime,
                        'numDocuments': numDocuments,
                        'feedbackDocuments': feedbackDocuments,
                        'progressDocuments': progressDocuments,
                        'database': appConfig['database'],
                        'collection': appConfig['collection'],
                        'numSegments': appConfig['numSegments']
                    }
                    save_checkpoint(checkpoint_file, checkpoint_data)
                    last_checkpoint_save = time.time()
                
                if (thisBoundary >= numBoundaries):
                    break

            if (numDocsTotal % progressDocuments == 0):
                pctDone = numDocsTotal/(numDocuments - feedbackDocuments)*100
                elapsedSecs = int(time.time() - queryStartTime)
                estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs)
                
                # Format elapsed time
                elapsedHours = elapsedSecs // 3600
                elapsedMins = (elapsedSecs % 3600) // 60
                elapsedSecsRem = elapsedSecs % 60
                elapsedStr = "{:d}h{:02d}m{:02d}s".format(elapsedHours, elapsedMins, elapsedSecsRem) if elapsedHours > 0 else "{:d}m{:02d}s".format(elapsedMins, elapsedSecsRem)
                
                # Format estimated time remaining
                estHours = estimatedSecsToDone // 3600
                estMins = (estimatedSecsToDone % 3600) // 60
                estSecsRem = estimatedSecsToDone % 60
                estStr = "{:d}h{:02d}m{:02d}s".format(estHours, estMins, estSecsRem) if estHours > 0 else "{:d}m{:02d}s".format(estMins, estSecsRem)
                
                boundaryPct = (numDocsBoundary/feedbackDocuments*100) if feedbackDocuments > 0 else 0
                print("  docs={:,} | pct={:.1f}% | elapsed={} | remaining={} | boundary={:,}/{:,} ({:.1f}%) | lastId={}".format(
                    numDocsTotal, pctDone, elapsedStr, estStr, numDocsBoundary, feedbackDocuments, boundaryPct, thisDoc["_id"]))
                    
    except (ConnectionFailure, ServerSelectionTimeoutError, NetworkTimeout, OperationFailure) as e:
        # Save checkpoint on error
        if checkpoint_file:
            error_startFromId = lastProcessedId if lastProcessedId else (startFromId if startFromId else (boundaryList[-1] if boundaryList else None))
            checkpoint_data = {
                'boundaries': boundaryList,
                'numDocsTotal': numDocsTotal,
                'numDocsBoundary': numDocsBoundary,
                'thisBoundary': thisBoundary,
                'startFromId': error_startFromId,
                'lastId': str(error_startFromId) if error_startFromId else None,
                'queryStartTime': queryStartTime,
                'numDocuments': numDocuments,
                'feedbackDocuments': feedbackDocuments,
                'progressDocuments': progressDocuments,
                'database': appConfig['database'],
                'collection': appConfig['collection'],
                'numSegments': appConfig['numSegments']
            }
            save_checkpoint(checkpoint_file, checkpoint_data)
            print("")
            print("Checkpoint saved to: {}".format(checkpoint_file))
            if lastProcessedId:
                print("Last processed _id: {}".format(lastProcessedId))
        
        print("")
        print("Connection/query error occurred. Progress so far:")
        print("  boundaries found: {}".format(len(boundaryList)))
        print("  documents processed: {}".format(numDocsTotal))
        print("  last boundary: {}".format(boundaryList[-1] if boundaryList else "none"))
        print("")
        if checkpoint_file:
            print("You can resume using the same command (checkpoint will be loaded automatically)")
        else:
            print("You can resume using:")
            print("  --resume-from '{}'".format("','".join(str(b) for b in boundaryList)))
        raise

    print("")

    # output full boundary list
    boundaryNum = 0
    print("Boundary list")
    for thisBoundary in boundaryList:
        boundaryNum += 1
        print("  boundary {:3d} - objectid {}".format(boundaryNum,thisBoundary))

    print("")

    boundaryListAsString = "{}".format(",".join('"{}"'.format(i) for i in boundaryList))
    print("boundaries as list | {}".format(boundaryListAsString))

    boundaryListAsStringForDms = "[{}]".format("],[".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list for DMS | {}".format(boundaryListAsStringForDms))

    print("")

    queryElapsedSecs = int(time.time() - queryStartTime)
    print('query required {} seconds'.format(queryElapsedSecs))

    print("")

    # Clean up checkpoint file on successful completion
    if checkpoint_file and os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print("Checkpoint file removed (completed successfully)")
        except Exception as e:
            print("Warning: Could not remove checkpoint file: {}".format(e))
        print("")

    client.close()


# ---------------------------------------------------------------------------
# Parallel mode (--parallel N)
#
# 1. Read the first and last _id (two indexed lookups).
# 2. Split the _id key space into parallel*4 ranges by interpolation
#    (ObjectId timestamp or integer value). No scanning needed for this.
# 3. Workers scan the ranges from a queue, one cursor each, projecting only
#    _id. Every --marker-interval documents a worker records (count, _id).
# 4. When all ranges are done, the per-range counts and markers are merged
#    and the N-1 boundaries are picked at the nearest marker.
#
# Load on the cluster is limited by: reading from a replica by default and
# refusing to run against the primary unless --allow-primary is passed, a
# shared rate limit across all workers, a hard cap on worker count, _id-only
# projection with a hinted _id index scan, and a small connection pool.
# ---------------------------------------------------------------------------

PARALLEL_MAX_WORKERS = 8
PARALLEL_RANGES_PER_WORKER = 4
PARALLEL_CHECKPOINT_VERSION = 1
PARALLEL_MAX_RETRIES = 8


class RateLimiter:
    """Token bucket shared by all workers. A rate of 0 or less means no limit."""

    def __init__(self, rate):
        self.rate = float(rate)
        self.tokens = self.rate
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, n, stop_event):
        if self.rate <= 0:
            return
        while not stop_event.is_set():
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= min(n, self.rate):
                    self.tokens -= n
                    return
                wait = (min(n, self.rate) - self.tokens) / self.rate
            stop_event.wait(min(wait, 1.0))


def _encode_id(value, id_type):
    if value is None:
        return None
    return str(value) if id_type == 'objectId' else int(value)


def _decode_id(value, id_type):
    if value is None:
        return None
    return ObjectId(value) if id_type == 'objectId' else int(value)


def _range_filter(lo, hi, after):
    cond = {}
    if after is not None:
        cond['$gt'] = after
    elif lo is not None:
        cond['$gte'] = lo
    if hi is not None:
        cond['$lt'] = hi
    return {'_id': cond} if cond else {}


def build_split_points(first_id, last_id, id_type, num_ranges):
    """Interpolate num_ranges-1 split points between first_id and last_id."""
    points = []
    if id_type == 'objectId':
        t0 = first_id.generation_time.timestamp()
        t1 = last_id.generation_time.timestamp()
        for i in range(1, num_ranges):
            ts = t0 + (t1 - t0) * i / num_ranges
            points.append(ObjectId.from_datetime(datetime.fromtimestamp(ts, tz=timezone.utc)))
    elif id_type == 'int':
        for i in range(1, num_ranges):
            points.append(first_id + (last_id - first_id) * i // num_ranges)
    else:
        raise ValueError("parallel mode supports objectId and int _id values only")
    return sorted(set(p for p in points if first_id < p <= last_id))


def pick_boundaries(ranges, num_segments, id_type):
    """Merge per-range counts and markers and choose the segment boundaries."""
    total = sum(r['count'] for r in ranges)
    positions = []
    ids = []
    offset = 0
    for r in ranges:
        for count, raw_id in r['markers']:
            positions.append(offset + count)
            ids.append(_decode_id(raw_id, id_type))
        offset += r['count']

    boundaries = []
    boundary_positions = []
    for k in range(1, num_segments):
        target = k * total / num_segments
        i = bisect.bisect_left(positions, target)
        if i >= len(positions):
            break
        if not boundaries or ids[i] > boundaries[-1]:
            boundaries.append(ids[i])
            boundary_positions.append(positions[i])
    return total, boundaries, boundary_positions


def save_parallel_checkpoint(path, state, lock):
    with lock:
        payload = json.dumps(state, indent=2)
    tmp_path = path + '.tmp'
    with open(tmp_path, 'w') as f:
        f.write(payload)
    os.replace(tmp_path, path)


def load_parallel_checkpoint(path, appConfig):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        state = json.load(f)
    if state.get('mode') != 'parallel' or state.get('version') != PARALLEL_CHECKPOINT_VERSION:
        print("Checkpoint {} was not written by parallel mode. Use a different --checkpoint-file.".format(path))
        sys.exit(1)
    for key, cfg_key in (('database', 'database'), ('collection', 'collection'), ('numSegments', 'numSegments')):
        if state.get(key) != appConfig[cfg_key]:
            print("Checkpoint {} is for {}={}, not {}. Use a different --checkpoint-file.".format(
                path, key, state.get(key), appConfig[cfg_key]))
            sys.exit(1)
    return state


def check_not_primary(client, allow_primary):
    """Refuse to scan the primary unless explicitly allowed."""
    try:
        hello = client.admin.command('hello')
    except OperationFailure:
        hello = client.admin.command('isMaster')
    topology = client.topology_description.topology_type_name
    has_secondary = len(client.secondaries) > 0
    on_primary_only = (topology == 'Single' and (hello.get('isWritablePrimary') or hello.get('ismaster'))) or \
                      (topology != 'Single' and not has_secondary)

    print("topology={} secondaries={} readPreference={}".format(
        topology, len(client.secondaries), client.read_preference.mongos_mode))

    if on_primary_only:
        msg = ("Reads would go to the primary instance. Point --uri at a replica instance, or at the "
               "cluster endpoint with replicaSet=rs0 and a replica in the cluster.")
        if not allow_primary:
            print(msg + " Pass --allow-primary to run anyway.")
            sys.exit(1)
        print("WARNING: " + msg + " Continuing because --allow-primary was passed.")


def via_parallel(appConfig):
    warnings.filterwarnings("ignore", "You appear to be connected to a DocumentDB cluster.")

    workers = appConfig['parallel']
    num_segments = appConfig['numSegments']
    batch_size = appConfig['batchSize']
    checkpoint_file = appConfig['checkpointFile']

    client = pymongo.MongoClient(
        host=appConfig['uri'],
        appname='segmentr-parallel',
        readPreference=appConfig['readPreference'],
        maxPoolSize=workers + 1,
        connectTimeoutMS=appConfig['connectTimeoutMS'],
        serverSelectionTimeoutMS=appConfig['serverSelectionTimeoutMS'],
        socketTimeoutMS=appConfig['socketTimeoutMS'],
    )
    check_not_primary(client, appConfig['allowPrimary'])
    if not check_for_mixed_types(appConfig):
        client.close()
        sys.exit(1)

    col = client[appConfig['database']][appConfig['collection']]

    lock = threading.Lock()
    state = load_parallel_checkpoint(checkpoint_file, appConfig)

    if state:
        id_type = state['idType']
        done = sum(1 for r in state['ranges'] if r['done'])
        print("Resuming from {}: {} of {} ranges done, {:,} documents counted".format(
            checkpoint_file, done, len(state['ranges']), sum(r['count'] for r in state['ranges'])))
    else:
        first = col.find_one({}, projection={'_id': True}, sort=[('_id', pymongo.ASCENDING)])
        last = col.find_one({}, projection={'_id': True}, sort=[('_id', pymongo.DESCENDING)])
        if first is None:
            print("Collection is empty")
            client.close()
            return
        first_id, last_id = first['_id'], last['_id']
        if isinstance(first_id, ObjectId):
            id_type = 'objectId'
        elif isinstance(first_id, int) and not isinstance(first_id, bool):
            id_type = 'int'
        else:
            print("Parallel mode supports objectId and int _id values only. Use --single-cursor instead.")
            client.close()
            sys.exit(1)

        estimated = col.estimated_document_count()
        marker_interval = appConfig['markerInterval'] or max(100, estimated // (num_segments * 1000))

        points = build_split_points(first_id, last_id, id_type, workers * PARALLEL_RANGES_PER_WORKER)
        edges = [None] + points + [None]
        state = {
            'mode': 'parallel',
            'version': PARALLEL_CHECKPOINT_VERSION,
            'database': appConfig['database'],
            'collection': appConfig['collection'],
            'numSegments': num_segments,
            'idType': id_type,
            'estimatedDocuments': estimated,
            'markerInterval': marker_interval,
            'complete': False,
            'ranges': [
                {'lo': _encode_id(edges[i], id_type), 'hi': _encode_id(edges[i + 1], id_type),
                 'lastId': None, 'count': 0, 'markers': [], 'done': False}
                for i in range(len(edges) - 1)
            ],
        }
        save_parallel_checkpoint(checkpoint_file, state, lock)
        print("collection {}.{} has about {:,} documents".format(
            appConfig['database'], appConfig['collection'], estimated))
        print("split _id space into {} ranges, {} workers, marker every {:,} documents".format(
            len(state['ranges']), workers, marker_interval))

    if not state['complete']:
        limiter = RateLimiter(appConfig['maxDocsPerSec'])
        stop_event = threading.Event()
        marker_interval = state['markerInterval']

        def scan_range(idx):
            r = state['ranges'][idx]
            lo = _decode_id(r['lo'], id_type)
            hi = _decode_id(r['hi'], id_type)
            retries = 0
            while not stop_event.is_set():
                count = r['count']
                last_id = _decode_id(r['lastId'], id_type)
                new_markers = []
                pending = 0
                try:
                    cursor = col.find(
                        _range_filter(lo, hi, last_id),
                        projection={'_id': True},
                        sort=[('_id', pymongo.ASCENDING)],
                    ).hint([('_id', pymongo.ASCENDING)]).batch_size(batch_size)
                    for doc in cursor:
                        count += 1
                        pending += 1
                        last_id = doc['_id']
                        if count == 1 or count % marker_interval == 0:
                            new_markers.append([count, _encode_id(last_id, id_type)])
                        if pending >= batch_size:
                            with lock:
                                r['count'] = count
                                r['lastId'] = _encode_id(last_id, id_type)
                                r['markers'].extend(new_markers)
                            new_markers = []
                            limiter.acquire(pending, stop_event)
                            pending = 0
                            retries = 0
                            if stop_event.is_set():
                                cursor.close()
                                return
                    with lock:
                        r['count'] = count
                        r['lastId'] = _encode_id(last_id, id_type)
                        r['markers'].extend(new_markers)
                        r['done'] = True
                    return
                except (ConnectionFailure, ServerSelectionTimeoutError, NetworkTimeout, OperationFailure) as e:
                    with lock:
                        r['count'] = count
                        r['lastId'] = _encode_id(last_id, id_type)
                        r['markers'].extend(new_markers)
                    retries += 1
                    if retries > PARALLEL_MAX_RETRIES:
                        raise
                    delay = min(60, 2 ** retries)
                    print("  range {} error: {}. retry {} in {}s".format(idx, e, retries, delay))
                    stop_event.wait(delay)

        pending_ranges = [i for i, r in enumerate(state['ranges']) if not r['done']]
        start = time.time()
        start_count = sum(r['count'] for r in state['ranges'])
        last_report = last_save = time.time()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        futures = {executor.submit(scan_range, i): i for i in pending_ranges}
        failed = None
        try:
            remaining = set(futures)
            while remaining:
                finished, remaining = concurrent.futures.wait(remaining, timeout=1)
                for f in finished:
                    if f.exception() is not None and failed is None:
                        failed = (futures[f], f.exception())
                        stop_event.set()
                now = time.time()
                if now - last_report >= 10:
                    with lock:
                        counted = sum(r['count'] for r in state['ranges'])
                        done = sum(1 for r in state['ranges'] if r['done'])
                    rate = (counted - start_count) / max(1, now - start)
                    est = state['estimatedDocuments']
                    pct = counted / est * 100 if est else 0
                    eta = (est - counted) / rate if rate > 0 and est > counted else 0
                    print("  docs={:,} | ~{:.1f}% | {:.0f} docs/sec | ranges {}/{} | eta ~{}m".format(
                        counted, pct, rate, done, len(state['ranges']), int(eta // 60)))
                    last_report = now
                if now - last_save >= 60:
                    save_parallel_checkpoint(checkpoint_file, state, lock)
                    last_save = now
        except KeyboardInterrupt:
            print("\nStopping workers...")
            stop_event.set()
            executor.shutdown(wait=True, cancel_futures=True)
            save_parallel_checkpoint(checkpoint_file, state, lock)
            print("Checkpoint saved to {}. Run the same command to resume.".format(checkpoint_file))
            client.close()
            sys.exit(130)

        executor.shutdown(wait=True)
        save_parallel_checkpoint(checkpoint_file, state, lock)
        if failed is not None:
            print("Range {} failed after {} retries: {}".format(failed[0], PARALLEL_MAX_RETRIES, failed[1]))
            print("Checkpoint saved to {}. Run the same command to resume.".format(checkpoint_file))
            client.close()
            sys.exit(1)

        state['complete'] = True
        save_parallel_checkpoint(checkpoint_file, state, lock)
        print("scan took {} seconds".format(int(time.time() - start)))
    else:
        print("Checkpoint is already complete, printing saved result.")

    client.close()

    total, boundaries, positions = pick_boundaries(state['ranges'], num_segments, id_type)
    print("")
    print("counted {:,} documents".format(total))
    print("Boundary list")
    previous = 0
    for i, (b, pos) in enumerate(zip(boundaries, positions), 1):
        print("  boundary {:3d} - {} {} | segment size ~{:,}".format(i, id_type, b, pos - previous))
        previous = pos
    print("  last segment size ~{:,}".format(total - previous))
    if len(boundaries) < num_segments - 1:
        print("WARNING: only found {} of {} boundaries. Lower --marker-interval or --num-segments.".format(
            len(boundaries), num_segments - 1))

    print("")
    print("boundaries as list | {}".format(",".join('"{}"'.format(b) for b in boundaries)))
    print("")
    print("boundaries as list for DMS | [{}]".format("],[".join('"{}"'.format(b) for b in boundaries)))
    print("")
    print("Checkpoint {} kept with the result. Delete it before scanning the collection again.".format(checkpoint_file))


def check_for_mixed_types(appConfig):
    # grab the first document and last document as ordered by _id, check for unsupported or differing data types
    returnValue = True

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    global supportedIdTypes

    connectTimeoutMS = appConfig.get('connectTimeoutMS', 30000)
    serverSelectionTimeoutMS = appConfig.get('serverSelectionTimeoutMS', 30000)
    socketTimeoutMS = appConfig.get('socketTimeoutMS', 300000)

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
    def get_client():
        return pymongo.MongoClient(
            host=appConfig['uri'],
            readPreference=appConfig.get('readPreference', 'primary'),
            connectTimeoutMS=connectTimeoutMS,
            serverSelectionTimeoutMS=serverSelectionTimeoutMS,
            socketTimeoutMS=socketTimeoutMS
        )

    @retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2)
    def get_id_types(col):
        idTypeFirst = col.aggregate([{"$sort":{"_id":pymongo.ASCENDING}},{"$project":{"_id":False,"idType":{"$type":"$_id"}}},{"$limit":1}]).next()['idType']
        idTypeLast = col.aggregate([{"$sort":{"_id":pymongo.DESCENDING}},{"$project":{"_id":False,"idType":{"$type":"$_id"}}},{"$limit":1}]).next()['idType']
        return idTypeFirst, idTypeLast

    client = get_client()
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    idTypeFirst, idTypeLast = get_id_types(col)

    if idTypeFirst not in supportedIdTypes:
        # unsupported data type
        print("Unsupported data type of '{}' for first _id value in {}.{} - only {} types are supported, stopping".format(idTypeFirst,appConfig['database'],appConfig['collection'],supportedIdTypes))
        returnValue = False

    if idTypeLast not in supportedIdTypes:
        # unsupported data type
        print("Unsupported data type of '{}' for first _id value in {}.{} - only {} types are supported, stopping".format(idTypeLast,appConfig['database'],appConfig['collection'],supportedIdTypes))
        returnValue = False

    if idTypeFirst != idTypeLast:
        # mixed data types
        print("Mixed data types of '{}' and '{}' for first and last  _id values in {}.{}, stopping".format(idTypeFirst,idTypeLast,appConfig['database'],appConfig['collection']))
        returnValue = False

    client.close()

    return returnValue


def main():
    parser = argparse.ArgumentParser(description='DMS Segment Analysis Tool.')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='URI')

    parser.add_argument('--database',
                        required=True,
                        type=str,
                        help='Database')

    parser.add_argument('--collection',
                        required=True,
                        type=str,
                        help='Collection')

    parser.add_argument('--num-segments',
                        required=True,
                        type=str,
                        help='Number of segments')

    parser.add_argument('--single-cursor',
                        required=False,
                        action='store_true',
                        help='Scan the full _id index using a cursor')

    parser.add_argument('--resume-from',
                        required=False,
                        type=str,
                        help='Comma-separated list of ObjectIds to resume from (e.g., "650000000000000000000001,660000000000000000000001")')

    parser.add_argument('--connect-timeout-ms',
                        required=False,
                        type=int,
                        default=30000,
                        help='Connection timeout in milliseconds (default: 30000)')

    parser.add_argument('--server-selection-timeout-ms',
                        required=False,
                        type=int,
                        default=30000,
                        help='Server selection timeout in milliseconds (default: 30000)')

    parser.add_argument('--socket-timeout-ms',
                        required=False,
                        type=int,
                        default=300000,
                        help='Socket timeout in milliseconds (default: 300000)')

    parser.add_argument('--checkpoint-file',
                        required=False,
                        type=str,
                        default=None,
                        help='Path to checkpoint file for saving/loading progress (default: auto-generated from database.collection)')

    parser.add_argument('--parallel',
                        required=False,
                        type=int,
                        default=0,
                        help='Scan with N parallel cursors (1-{}). Reads from a replica and is rate limited. '
                             'Supports objectId and int _id only'.format(PARALLEL_MAX_WORKERS))

    parser.add_argument('--max-docs-per-sec',
                        required=False,
                        type=int,
                        default=20000,
                        help='Parallel mode: total documents per second across all workers, 0 for no limit (default: 20000)')

    parser.add_argument('--batch-size',
                        required=False,
                        type=int,
                        default=5000,
                        help='Parallel mode: cursor batch size (default: 5000)')

    parser.add_argument('--read-preference',
                        required=False,
                        default=None,
                        choices=['secondary', 'secondaryPreferred', 'primaryPreferred', 'primary'],
                        help='Parallel mode: read preference (default: secondary, or secondaryPreferred '
                             'with --allow-primary). secondary never falls back to the primary')

    parser.add_argument('--allow-primary',
                        required=False,
                        action='store_true',
                        help='Parallel mode: allow the scan to run when reads would go to the primary')

    parser.add_argument('--marker-interval',
                        required=False,
                        type=int,
                        default=0,
                        help='Parallel mode: record an _id every N documents (default: about 0.1%% of a segment)')

    args = parser.parse_args()

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['database'] = args.database
    appConfig['collection'] = args.collection
    appConfig['numSegments'] = int(args.num_segments)
    appConfig['connectTimeoutMS'] = args.connect_timeout_ms
    appConfig['serverSelectionTimeoutMS'] = args.server_selection_timeout_ms
    appConfig['socketTimeoutMS'] = args.socket_timeout_ms

    if args.parallel:
        if not 1 <= args.parallel <= PARALLEL_MAX_WORKERS:
            print("--parallel must be between 1 and {}".format(PARALLEL_MAX_WORKERS))
            sys.exit(1)
        if args.single_cursor or args.resume_from:
            print("--parallel can't be combined with --single-cursor or --resume-from")
            sys.exit(1)
        if args.batch_size < 1 or args.max_docs_per_sec < 0 or args.marker_interval < 0:
            print("--batch-size must be positive, --max-docs-per-sec and --marker-interval can't be negative")
            sys.exit(1)
        appConfig['parallel'] = args.parallel
        appConfig['maxDocsPerSec'] = args.max_docs_per_sec
        appConfig['batchSize'] = args.batch_size
        appConfig['readPreference'] = args.read_preference or ('secondaryPreferred' if args.allow_primary else 'secondary')
        if appConfig['readPreference'] in ('primary', 'primaryPreferred') and not args.allow_primary:
            print("--read-preference {} reads from the primary. Pass --allow-primary to confirm.".format(
                appConfig['readPreference']))
            sys.exit(1)
        appConfig['allowPrimary'] = args.allow_primary
        appConfig['markerInterval'] = args.marker_interval
        appConfig['checkpointFile'] = args.checkpoint_file or "checkpoint_{}_{}_{}_parallel.json".format(
            args.database, args.collection, args.num_segments)
        via_parallel(appConfig)
        return

    # Set checkpoint file (auto-generate if not provided)
    if args.checkpoint_file:
        appConfig['checkpointFile'] = args.checkpoint_file
    else:
        # Auto-generate checkpoint filename: checkpoint_<database>_<collection>_<numSegments>.json
        checkpoint_filename = "checkpoint_{}_{}_{}.json".format(
            args.database, args.collection, args.num_segments
        )
        appConfig['checkpointFile'] = checkpoint_filename

    # Parse resume from boundaries
    if args.resume_from:
        try:
            # Split by comma and convert to ObjectIds
            boundary_strings = [b.strip() for b in args.resume_from.split(',')]
            appConfig['resumeFromBoundaries'] = [ObjectId(b) if len(b) == 24 else b for b in boundary_strings]
            print("Resume mode enabled with {} boundaries".format(len(appConfig['resumeFromBoundaries'])))
        except Exception as e:
            print("Error parsing --resume-from: {}. Expected comma-separated ObjectIds.".format(e))
            sys.exit(1)
    else:
        appConfig['resumeFromBoundaries'] = []

    if check_for_mixed_types(appConfig):
        if args.single_cursor:
            via_cursor(appConfig)

        else:
            via_skips(appConfig)


if __name__ == "__main__":
    main()

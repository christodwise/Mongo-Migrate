import subprocess
import os
import time
import json
import shutil
from pymongo import MongoClient

def run_command(command, log_callback, redact_patterns=None):
    """Executes a command and streams output to log_callback with redaction."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    for line in process.stdout:
        output = line.strip()
        if output:
            # Redact sensitive information
            if redact_patterns:
                for pattern in redact_patterns:
                    output = output.replace(pattern, "******")
            log_callback(output)
        
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed with exit code {process.returncode}")

def migrate_db(source, target, log_callback, is_instance=False):
    """Performs actual MongoDB migration using mongodump and mongorestore."""
    temp_dir = f"/tmp/migration_{int(time.time())}"
    os.makedirs(temp_dir, exist_ok=True)
    
    redact_patterns = []
    if source.get('uri'): redact_patterns.append(source['uri'])
    if target.get('uri'): redact_patterns.append(target['uri'])
    
    try:
        # Phase 1: Dump
        log_callback("PHASE:DUMPING|Connecting to source and generating dump...")
        dump_cmd = ["mongodump", "--uri", source['uri'], "--out", temp_dir]
        
        if not is_instance:
            dump_cmd.extend(["--db", source['dbname']])
            log_callback(f"Targeting database: {source['dbname']}")
        else:
            log_callback("Full instance migration initiated (Mirror Mode)")
            # Exclude system databases for full instance dump
            # mongodump doesn't have an exclude-db flag easily for full dump, 
            # but it usually skips admin/config/local unless specified.
            pass

        run_command(dump_cmd, log_callback, redact_patterns)
        log_callback("Dump completed successfully.")
        
        # Phase 2: Restore
        log_callback("PHASE:RESTORING|Preparing target for data injection...")
        restore_cmd = ["mongorestore", "--uri", target['uri'], "--drop"]
        
        if not is_instance:
            # In single DB mode, we map the source DB to target DB
            # The dump structure is temp_dir/source_db/collections...
            source_path = os.path.join(temp_dir, source['dbname'])
            if not os.path.exists(source_path):
                # Fallback check if mongodump behaved differently
                log_callback(f"ERROR: Dump path not found at {source_path}")
                raise Exception("Dump directory missing")
                
            restore_cmd.extend(["--nsInclude", f"{source['dbname']}.*"])
            # If target db name is different, use nsFrom/nsTo
            if source['dbname'] != target['dbname']:
                restore_cmd.extend([
                    "--nsFrom", f"{source['dbname']}.*",
                    "--nsTo", f"{target['dbname']}.*"
                ])
            restore_cmd.append(temp_dir) # mongorestore takes the root dump dir usually or specific db dir
        else:
            log_callback("Restoring all databases to target...")
            restore_cmd.append(temp_dir)

        run_command(restore_cmd, log_callback, redact_patterns)
        log_callback("Restore completed successfully.")
        
        return True, "Migration completed successfully!"
    except Exception as e:
        log_callback(f"ERROR: Migration Failed - {str(e)}")
        return False, str(e)
    finally:
        # Cleanup
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except:
            pass

def test_connection(uri):
    """Tests connection and returns MongoDB version."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        server_info = client.server_info()
        version = server_info.get('version', 'Unknown')
        return True, f"MongoDB {version}"
    except Exception as e:
        return False, str(e)

def get_db_stats(uri, dbname, is_instance=False):
    """Returns basic stats about the database or instance."""
    try:
        client = MongoClient(uri)
        if is_instance:
            # Aggregate stats for all DBs
            dbs = client.list_database_names()
            ignore = ['admin', 'config', 'local']
            total_collections = 0
            total_objects = 0
            for db_name in dbs:
                if db_name in ignore: continue
                db = client[db_name]
                try:
                    stats = db.command('dbStats')
                    total_collections += stats.get('collections', 0)
                    total_objects += stats.get('objects', 0)
                except:
                    continue
            return {
                'collections': total_collections,
                'objects': total_objects,
                'dbCount': len([d for d in dbs if d not in ignore])
            }
        else:
            db = client[dbname]
            stats = db.command('dbStats')
            return {
                'collections': stats.get('collections', 0),
                'objects': stats.get('objects', 0),
                'dataSize': stats.get('dataSize', 0),
                'storageSize': stats.get('storageSize', 0)
            }
    except Exception as e:
        raise e

def preflight_check(source, target):
    """Runs checks before migration."""
    checks = []
    
    # Check 1: Source Connectivity
    s_ok, s_msg = test_connection(source['uri'])
    if s_ok:
        checks.append({'status': 'pass', 'msg': f"Source Context: {s_msg}"})
    else:
        checks.append({'status': 'fail', 'msg': f"Source Offline: {s_msg}"})
        return checks
        
    # Check 2: Target Connectivity
    t_ok, t_msg = test_connection(target['uri'])
    if t_ok:
        checks.append({'status': 'pass', 'msg': f"Target Context: {t_msg}"})
    else:
        checks.append({'status': 'fail', 'msg': f"Target Offline: {t_msg}"})
    
    return checks

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
        client = MongoClient(source['uri'])
        
        if is_instance:
            log_callback("PHASE:DISCOVERY|Detecting non-system databases...")
            dbs = client.list_database_names()
            ignore = ['admin', 'config', 'local']
            target_dbs = [d for d in dbs if d not in ignore]
            log_callback(f"Detected {len(target_dbs)} databases: {', '.join(target_dbs)}")
            
            # Phase 1: Dump Full Instance (excluding systems)
            log_callback("PHASE:DUMPING|Capturing data and metadata (All DBs)...")
            dump_cmd = ["mongodump", "--uri", source['uri'], "--out", temp_dir]
            # mongodump does not easily exclude DBs in one go without specifying each, 
            # but usually skips systems. We'll dump everything and rely on restore filtering.
            run_command(dump_cmd, log_callback, redact_patterns)
            
            # Optional: Capture Users and Roles from admin if they exist
            log_callback("PHASE:AUTH|Capturing system users and roles...")
            auth_dump_cmd = ["mongodump", "--uri", source['uri'], "--db", "admin", "--out", temp_dir]
            try:
                run_command(auth_dump_cmd, log_callback, redact_patterns)
            except:
                log_callback("Note: Skipping granular admin auth dump (may lack permissions).")

            log_callback("Dump completed successfully.")
            
            # Phase 2: Restore Full Instance
            log_callback("PHASE:RESTORING|Injecting data and metadata to destination...")
            restore_cmd = ["mongorestore", "--uri", target['uri'], "--drop", temp_dir]
            run_command(restore_cmd, log_callback, redact_patterns)
            log_callback("Restore completed successfully.")

        else:
            # Single DB Mode
            log_callback(f"PHASE:DUMPING|Capturing {source['dbname']} context...")
            dump_cmd = ["mongodump", "--uri", source['uri'], "--db", source['dbname'], "--out", temp_dir]
            run_command(dump_cmd, log_callback, redact_patterns)
            log_callback("Dump completed successfully.")
            
            log_callback(f"PHASE:RESTORING|Injecting to destination node: {target['dbname']}...")
            restore_cmd = ["mongorestore", "--uri", target['uri'], "--drop"]
            
            source_path = os.path.join(temp_dir, source['dbname'])
            if not os.path.exists(source_path):
                raise Exception(f"Dump path not found at {source_path}")
                
            restore_cmd.extend(["--nsInclude", f"{source['dbname']}.*"])
            if source['dbname'] != target['dbname']:
                restore_cmd.extend([
                    "--nsFrom", f"{source['dbname']}.*",
                    "--nsTo", f"{target['dbname']}.*"
                ])
            restore_cmd.append(temp_dir)
            run_command(restore_cmd, log_callback, redact_patterns)
            log_callback("Restore completed successfully.")

        # Phase 3: Validation
        log_callback("PHASE:VALIDATION|Executing post-migration audit...")
        target_client = MongoClient(target['uri'])
        if is_instance:
            s_dbs = len([d for d in client.list_database_names() if d not in ['admin', 'config', 'local']])
            t_dbs = len([d for d in target_client.list_database_names() if d not in ['admin', 'config', 'local']])
            log_callback(f"Validation: Source DBs ({s_dbs}) vs Destination DBs ({t_dbs})")
        else:
            s_cols = len(client[source['dbname']].list_collection_names())
            t_cols = len(target_client[target['dbname']].list_collection_names())
            log_callback(f"Validation: Source Collections ({s_cols}) vs Destination Collections ({t_cols})")
            
        return True, "Migration completed successfully!"
    except Exception as e:
        log_callback(f"ERROR: Migration Failed - {str(e)}")
        return False, str(e)
    finally:
        try:
            if os.path.exists(temp_dir):
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

import subprocess
import os
import time
import json
import shutil
from pymongo import MongoClient
from urllib.parse import urlparse, urlunparse

def get_base_uri(uri):
    """Surgically strips the database name from a MongoDB URI for shell compatibility."""
    if not uri: return uri
    try:
        parsed = urlparse(uri)
        # Reconstruct without path (database name), keeping all query options
        # path character '/' is ignored if provided as the third argument
        new_uri = urlunparse((
            parsed.scheme,
            parsed.netloc,
            '/', 
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        return new_uri
    except:
        return uri

def run_command(command, log_callback, redact_patterns=None):
    """Executes a command and streams output to log_callback with redaction."""
    # Debug: Log the command (redacted)
    cmd_str = " ".join(command)
    if redact_patterns:
        for p in redact_patterns:
            cmd_str = cmd_str.replace(p, "******")
    
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
            if redact_patterns:
                for pattern in redact_patterns:
                    output = output.replace(pattern, "******")
            log_callback(output)
        
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed with exit code {process.returncode}")

def migrate_db(source, target, log_callback, is_instance=False):
    """Performs MongoDB migration using mongodump and mongorestore."""
    temp_dir = f"/tmp/migration_{int(time.time())}"
    os.makedirs(temp_dir, exist_ok=True)
    
    redact_patterns = []
    if source.get('uri'): redact_patterns.append(source['uri'])
    if target.get('uri'): redact_patterns.append(target['uri'])
    
    # Base URIs for shell commands (Strictly stripping DB to avoid conflict with --db)
    source_uri = get_base_uri(source['uri'])
    target_uri = get_base_uri(target['uri'])
    
    # Add base URIs to redaction patterns
    if source_uri not in redact_patterns: redact_patterns.append(source_uri)
    if target_uri not in redact_patterns: redact_patterns.append(target_uri)
    
    try:
        client = MongoClient(source['uri'])
        
        if is_instance:
            log_callback("PHASE:DISCOVERY|Detecting instance databases...")
            dbs = client.list_database_names()
            ignore = ['admin', 'config', 'local']
            target_dbs = [d for d in dbs if d not in ignore]
            log_callback(f"Found {len(target_dbs)} databases. Initializing Full Sync...")
            
            # Phase 1: Full Instance Dump
            # Instance dump skips local/config automatically and handles users/roles if dumped from root
            log_callback("PHASE:DUMPING|Capturing Full Instance (All DBs + Metadata)...")
            dump_cmd = ["mongodump", "--uri", source_uri, "--out", temp_dir]
            run_command(dump_cmd, log_callback, redact_patterns)
            
            # Phase 2: Full Instance Restore
            log_callback("PHASE:RESTORING|Injecting Instance to Destination...")
            restore_cmd = ["mongorestore", "--uri", target_uri, "--drop", temp_dir]
            run_command(restore_cmd, log_callback, redact_patterns)
            
            log_callback("Full Instance synchronization complete.")

        else:
            # Single DB Mode
            log_callback(f"PHASE:MIGRATING|Context Mapping: {source['dbname']} -> {target['dbname']}")
            
            # Dump single DB
            dump_cmd = ["mongodump", "--uri", source_uri, "--db", source['dbname'], "--out", temp_dir]
            run_command(dump_cmd, log_callback, redact_patterns)
            
            # Restore single DB (handling potential rename)
            restore_cmd = ["mongorestore", "--uri", target_uri, "--drop"]
            source_path = os.path.join(temp_dir, source['dbname'])
            
            if not os.path.exists(source_path):
                raise Exception(f"Dump verification failed at {source_path}")
                
            restore_cmd.extend(["--nsInclude", f"{source['dbname']}.*"])
            if source['dbname'] != target['dbname']:
                restore_cmd.extend(["--nsFrom", f"{source['dbname']}.*", "--nsTo", f"{target['dbname']}.*"])
            
            restore_cmd.append(temp_dir)
            run_command(restore_cmd, log_callback, redact_patterns)
            
            log_callback("Database context synchronization complete.")

        # Final Verification Phase
        log_callback("PHASE:VALIDATION|Executing integrity check...")
        target_client = MongoClient(target['uri'])
        if is_instance:
            t_dbs = [d for d in target_client.list_database_names() if d not in ['admin', 'config', 'local']]
            log_callback(f"PHASE:SUCCESS|Full Sync Verified: {len(t_dbs)} databases active on target.")
        else:
            t_cols = len(target_client[target['dbname']].list_collection_names())
            log_callback(f"PHASE:SUCCESS|Mapping Verified: {t_cols} collections active on target context.")
            
        return True, "Migration completed successfully!"
    except Exception as e:
        log_callback(f"ERROR: Migration Stopped - {str(e)}")
        return False, str(e)
    finally:
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except: pass

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

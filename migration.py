import subprocess
import os
import time
import json
import shutil
from pymongo import MongoClient
from urllib.parse import urlparse, urlunparse

def get_base_uri(uri):
    """Robustly strips the database name from a MongoDB URI for shell command compatibility."""
    if not uri: return uri
    try:
        # Standard MongoDB URI: mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultdb][?options]]
        # We need to find the part after the hosts and before the options.
        
        # 1. Handle query parameters first
        base_part = uri.split('?')[0]
        options_part = f"?{uri.split('?')[1]}" if '?' in uri else ""
        
        # 2. Find the host section (after //)
        if '//' in base_part:
            schema, rest = base_part.split('//', 1)
            # Find the first slash after the host/identity section
            if '/' in rest:
                host_section, _ = rest.split('/', 1)
                # Reconstruct: schema + // + host_section + / + options
                # The trailing slash ensures mongodump/restore treat it as a base URI
                return f"{schema}//{host_section}/{options_part}"
            else:
                # No database specified, just ensure a trailing slash for safety
                return f"{base_part}/{options_part}"
        return uri
    except:
        return uri

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
    
    # Base URIs for shell commands (stripping DB names to avoid conflict with --db flag)
    source_uri = get_base_uri(source['uri'])
    target_uri = get_base_uri(target['uri'])
    
    # Add base URIs to redaction patterns as well
    if source_uri not in redact_patterns: redact_patterns.append(source_uri)
    if target_uri not in redact_patterns: redact_patterns.append(target_uri)
    
    try:
        client = MongoClient(source['uri'])
        
        if is_instance:
            log_callback("PHASE:DISCOVERY|Detecting non-system databases...")
            dbs = client.list_database_names()
            ignore = ['admin', 'config', 'local']
            target_dbs = [d for d in dbs if d not in ignore]
            log_callback(f"Detected {len(target_dbs)} databases: {', '.join(target_dbs)}")
            
            # 1. Sync Authentication Data (Users/Roles)
            log_callback("PHASE:AUTH|Synchronizing System Credentials...")
            auth_cmd_dump = ["mongodump", "--uri", source_uri, "--db", "admin", "--out", temp_dir]
            try:
                run_command(auth_cmd_dump, log_callback, redact_patterns)
                auth_cmd_restore = ["mongorestore", "--uri", target_uri, os.path.join(temp_dir, "admin")]
                run_command(auth_cmd_restore, log_callback, redact_patterns)
                log_callback("System credentials synchronized.")
            except Exception as auth_err:
                log_callback(f"Note: Auth sync skipped or failed ({str(auth_err)})")

            # 2. Iterate and Sync each Database
            for db_name in target_dbs:
                log_callback(f"PHASE:SYNCING|Processing Database: {db_name}")
                
                # Dump
                db_temp = os.path.join(temp_dir, db_name)
                dump_cmd = ["mongodump", "--uri", source_uri, "--db", db_name, "--out", temp_dir]
                run_command(dump_cmd, log_callback, redact_patterns)
                
                # Restore
                restore_cmd = ["mongorestore", "--uri", target_uri, "--db", db_name, "--drop", db_temp]
                run_command(restore_cmd, log_callback, redact_patterns)
                
                log_callback(f"Database {db_name} synchronized successfully.")

            log_callback("PHASE:VALIDATION|Finalizing Full Instance Sync...")

        else:
            # Single DB Mode
            log_callback(f"PHASE:MIGRATING|Context: {source['dbname']} -> {target['dbname']}")
            
            dump_cmd = ["mongodump", "--uri", source_uri, "--db", source['dbname'], "--out", temp_dir]
            run_command(dump_cmd, log_callback, redact_patterns)
            
            restore_cmd = ["mongorestore", "--uri", target_uri, "--drop"]
            source_path = os.path.join(temp_dir, source['dbname'])
            if not os.path.exists(source_path):
                raise Exception(f"Dump path not found at {source_path}")
                
            restore_cmd.extend(["--nsInclude", f"{source['dbname']}.*"])
            if source['dbname'] != target['dbname']:
                restore_cmd.extend(["--nsFrom", f"{source['dbname']}.*", "--nsTo", f"{target['dbname']}.*"])
            restore_cmd.append(temp_dir)
            
            run_command(restore_cmd, log_callback, redact_patterns)
            log_callback("Context synchronization complete.")

        # Final Audit
        target_client = MongoClient(target['uri'])
        if is_instance:
            t_dbs = [d for d in target_client.list_database_names() if d not in ['admin', 'config', 'local']]
            log_callback(f"PHASE:SUCCESS|Full Sync Verified: {len(t_dbs)} databases active on destination.")
        else:
            t_cols = len(target_client[target['dbname']].list_collection_names())
            log_callback(f"PHASE:SUCCESS|Mapping Verified: {t_cols} collections active on destination context.")
            
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

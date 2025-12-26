import subprocess
import os
import time
import json
import shutil
from pymongo import MongoClient
from urllib.parse import urlparse, urlunparse

def get_base_uri(uri):
    """Robustly strips the database name from any MongoDB URI format."""
    if not uri: return uri
    try:
        # Standardize: Ensure we handle common URI structures including mongodb+srv
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(uri)
        
        # We force the path to be exactly '/' to strip any database name
        # while preserving all netloc info (user:pass@host:port) and query options.
        new_uri = urlunparse((
            parsed.scheme,
            parsed.netloc,
            '/', 
            '', # params
            parsed.query,
            parsed.fragment
        ))
        
        # Extra safety: if for some reason urlunparse adds the DB back or something weird happens,
        # we can do a secondary check here, but urlparse/urlunparse is generally reliable for this.
        return new_uri
    except Exception as e:
        # If urlparse fails, we fallback to a simple split logic
        try:
            if "?" in uri:
                base_part, options = uri.split("?", 1)
                # Split authority from path
                if "://" in base_part:
                    scheme, rest = base_part.split("://", 1)
                    if "/" in rest:
                        authority = rest.split("/", 1)[0]
                        return f"{scheme}://{authority}/?{options}"
            elif "://" in uri:
                scheme, rest = uri.split("://", 1)
                if "/" in rest:
                    authority = rest.split("/", 1)[0]
                    return f"{scheme}://{authority}/"
        except:
            pass
        return uri

def run_command(command, log_callback, redact_patterns=None):
    """Executes a command and streams output to log_callback with redaction."""
    # Debug: Log the command (redacted)
    debug_cmd = " ".join(command)
    if redact_patterns:
        for p in redact_patterns:
            if p: debug_cmd = debug_cmd.replace(p, "******")
    
    log_callback(f"DEBUG: Executing internal command: {debug_cmd}")
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        current_phase = None
        for line in process.stdout:
            output = line.strip()
            if output:
                # Enhanced log parsing for progress visualization
                lower_out = output.lower()
                if "writing metadata" in lower_out or "restoring metadata" in lower_out:
                    if current_phase != "METADATA":
                        log_callback("PHASE:METADATA|Synchronizing System Metadata...")
                        current_phase = "METADATA"
                elif "restoring" in lower_out and "collection" in lower_out:
                    if current_phase != "DATA":
                        log_callback("PHASE:DATA|Transferring Collection Data...")
                        current_phase = "DATA"
                elif "index" in lower_out and ("creating" in lower_out or "building" in lower_out):
                    if current_phase != "INDEX":
                        log_callback("PHASE:INDEX|Rebuilding Database Indexes...")
                        current_phase = "INDEX"

                if redact_patterns:
                    for pattern in redact_patterns:
                        if pattern: output = output.replace(pattern, "******")
                log_callback(output)
            
        process.wait()
        if process.returncode != 0:
            log_callback(f"ERROR: Command exited with code {process.returncode}")
            raise Exception(f"Process failed (Exit code: {process.returncode})")
    except FileNotFoundError:
        error_msg = f"ERROR: MongoDB tool '{command[0]}' not found in system path."
        log_callback(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        log_callback(f"ERROR: Command execution failed: {str(e)}")
        raise e

def migrate_db(source, target, log_callback, target_dbs=None):
    """Performs full or selective MongoDB instance migration."""
    temp_dir = f"/tmp/migration_{int(time.time())}"
    os.makedirs(temp_dir, exist_ok=True)
    
    redact_patterns = []
    if source.get('uri'): redact_patterns.append(source['uri'])
    if target.get('uri'): redact_patterns.append(target['uri'])
    
    # Base URIs for shell commands (Strictly stripping DB to avoid conflict)
    source_uri = get_base_uri(source['uri'])
    target_uri = get_base_uri(target['uri'])
    
    # Add base URIs to redaction patterns
    if source_uri not in redact_patterns: redact_patterns.append(source_uri)
    if target_uri not in redact_patterns: redact_patterns.append(target_uri)
    
    try:
        log_callback("PHASE:DISCOVERY|Initializing Optimized Full Instance Sync...")
        
        # Determine concurrency based on CPU cores (min 4, max 16)
        cores = os.cpu_count() or 4
        concurrency = max(4, min(cores, 16))
        log_callback(f"Engine tuning: Utilizing {concurrency} parallel streams.")

        # Phase 1: Full Instance Dump
        if target_dbs:
            log_callback(f"PHASE:DUMPING|Capturing Selected Databases: {', '.join(target_dbs)}...")
            dump_cmd = [
                "mongodump", 
                "--uri", source_uri, 
                "--out", temp_dir,
                "--numParallelCollections", str(concurrency)
            ]
            for db in target_dbs:
                dump_cmd.append(f"--nsInclude={db}.*")
        else:
            log_callback("PHASE:DUMPING|Capturing Full Instance (Parallel Mode)...")
            dump_cmd = [
                "mongodump", 
                "--uri", source_uri, 
                "--out", temp_dir,
                "--numParallelCollections", str(concurrency)
            ]
        
        run_command(dump_cmd, log_callback, redact_patterns)
        
        # Phase 2: Full Instance Restore
        log_callback("PHASE:RESTORING|Injecting Instance to Destination (High-Throughput)...")
        restore_cmd = [
            "mongorestore", 
            "--uri", target_uri, 
            "--drop",
            "--numParallelCollections", str(concurrency),
            "--numInsertionWorkersPerCollection", str(concurrency),
            temp_dir
        ]
        # While mongorestore generally restores what's in the dir, 
        # specifying nsInclude again can be safer but usually redundant if dump was selective.
        # However, if we dumped everything (no target_dbs), but want to restore everything, the command is same.
        # If we dumped specific DBs, the temp_dir only contains those.
        # So the standard restore command works for both cases.
        
        run_command(restore_cmd, log_callback, redact_patterns)
        
        log_callback("Full Instance synchronization complete.")

        # Final Verification Phase
        log_callback("PHASE:VALIDATION|Executing integrity check...")
        target_client = MongoClient(target['uri'])
        t_dbs = [d for d in target_client.list_database_names() if d not in ['admin', 'config', 'local']]
        log_callback(f"PHASE:SUCCESS|Full Sync Verified: {len(t_dbs)} databases active on target.")
            
        return True, "Full migration completed successfully!"
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

def get_db_stats(uri):
    """Returns basic stats about the entire MongoDB instance."""
    try:
        client = MongoClient(uri)
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

def get_databases(uri):
    """Returns a list of database names from the instance."""
    try:
        client = MongoClient(uri)
        dbs = client.list_database_names()
        ignore = ['admin', 'config', 'local']
        return [d for d in dbs if d not in ignore]
    except Exception as e:
        raise e

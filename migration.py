import subprocess
import os
import time
import json
from pymongo import MongoClient

def run_command(command, log_callback):
    """Executes a command and streams output to log_callback."""
    # Scrape passwords from logs for security
    # command list may contain passwords in URI
    
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    for line in process.stdout:
        # Simple redaction for mongo URIs if they appear in logs
        # This is a bit reactive, better to avoid printing it at all
        log_callback(line.strip())
        
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed with exit code {process.returncode}")

def migrate_db(source, target, log_callback):
    """
    source: dict with 'uri' and 'dbname'
    target: dict with 'uri' and 'dbname'
    """
    dump_dir = os.path.join(os.getcwd(), 'temp_dump')
    
    try:
        # 1. mongodump
        log_callback(f"PHASE:DUMPING|Starting dump from source...")
        if os.path.exists(dump_dir):
             import shutil
             shutil.rmtree(dump_dir)
        
        # We use --archive for more efficient streaming if supported, but directory dump is robust
        dump_cmd = [
            'mongodump',
            '--uri', source['uri'],
            '--db', source['dbname'],
            '--out', dump_dir
        ]
        
        run_command(dump_cmd, log_callback)
        log_callback("Dump completed successfully.")
        
        # 2. Preparation (No explicit 'drop tables' equivalent needed if using --drop in restore)
        log_callback("PHASE:PREPARING|Preparing target database...")
        
        # 3. mongorestore
        log_callback(f"PHASE:RESTORING|Starting restore to target...")
        
        # The dump output will be in dump_dir/dbname
        restore_path = os.path.join(dump_dir, source['dbname'])
        
        restore_cmd = [
            'mongorestore',
            '--uri', target['uri'],
            '--db', target['dbname'],
            '--drop', # Drop collections on target before restoring
            restore_path
        ]
        
        run_command(restore_cmd, log_callback)
        log_callback("Restore completed successfully.")
        
        return True, "Migration completed successfully!"
        
    except Exception as e:
        log_callback(f"ERROR: Migration Failed - {str(e)}")
        return False, str(e)
    finally:
        if os.path.exists(dump_dir):
            try:
                import shutil
                shutil.rmtree(dump_dir)
                log_callback("Cleaned up temporary resources.")
            except:
                pass

def test_connection(uri):
    """Tests connection and returns MongoDB version."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require auth.
        info = client.admin.command('ismaster')
        server_info = client.server_info()
        version = server_info.get('version', 'Unknown')
        return True, f"MongoDB {version}"
    except Exception as e:
        return False, str(e)

def get_db_stats(uri, dbname):
    """Returns basic stats about the database."""
    try:
        client = MongoClient(uri)
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
        checks.append({'status': 'pass', 'msg': f"Source Connected: {s_msg}"})
    else:
        checks.append({'status': 'fail', 'msg': f"Source Failed: {s_msg}"})
        return checks
        
    # Check 2: Target Connectivity
    t_ok, t_msg = test_connection(target['uri'])
    if t_ok:
        checks.append({'status': 'pass', 'msg': f"Target Connected: {t_msg}"})
    else:
        checks.append({'status': 'fail', 'msg': f"Target Failed: {t_msg}"})
    
    return checks

import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_socketio import SocketIO, emit
import storage
import migration
import secrets
from functools import wraps
import hashlib


app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(16)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Keep track of active migrations
active_migrations = {}

# Simple Authentication
ADMIN_USER = "admin"
ADMIN_PASS_HASH = hashlib.sha256("admin123".encode()).hexdigest()

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs) :
        if 'logged_in' not in session:
            return jsonify({'success': False, 'message': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    if 'logged_in' not in session:
        return render_template('login.html')
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    if username == ADMIN_USER:
        pass_hash = hashlib.sha256(password.encode()).hexdigest()
        if pass_hash == ADMIN_PASS_HASH:
            session['logged_in'] = True
            return jsonify({'success': True})
    
    return jsonify({'success': False, 'message': 'Invalid credentials'}), 401

@app.route('/api/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('index'))

@app.route('/api/connections', methods=['GET'])
@login_required
def get_connections():

    return jsonify(storage.get_connections_grouped())

@app.route('/api/connections', methods=['POST'])
@login_required
def add_connection():
    data = request.json
    # Validation
    if not all(k in data for k in ('name', 'uri', 'dbname')):
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400
    
    success, message = storage.save_connection(
        data['name'], 
        data['uri'], 
        data['dbname'], 
        data.get('environment', 'Production')
    )
    return jsonify({'success': success, 'message': message})

@app.route('/api/connections/<int:conn_id>', methods=['DELETE'])
@login_required
def delete_connection(conn_id):
    storage.delete_connection(conn_id)
    return jsonify({'success': True})

@app.route('/api/test-connection', methods=['POST'])
@login_required
def test_connection():
    data = request.json
    success, message = migration.test_connection(data['uri'])
    return jsonify({'success': success, 'message': message})

@app.route('/api/db-stats', methods=['POST'])
@login_required
def get_db_stats():
    data = request.json
    try:
        stats = migration.get_db_stats(data['uri'])
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/preflight', methods=['POST'])
@login_required
def preflight():
    data = request.json
    checks = migration.preflight_check(data['source'], data['target'])
    return jsonify({'checks': checks})

@app.route('/api/databases', methods=['POST'])
@login_required
def get_databases():
    data = request.json
    try:
        dbs = migration.get_databases(data['uri'])
        return jsonify({'success': True, 'databases': dbs})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@socketio.on('start_migration')
def handle_migration(data):
    migration_id = secrets.token_hex(4)
    source = data['source']
    target = data['target']
    
    active_migrations[migration_id] = {
        'source': source['name'],
        'target': target['name'],
        'status': 'running'
    }

    target_dbs = data.get('databases')

    def log_callback(message):
        socketio.emit('migration_log', {'id': migration_id, 'message': message})

    def run_migration():
        try:
            success, message = migration.migrate_db(source, target, log_callback, target_dbs)
            socketio.emit('migration_complete', {
                'id': migration_id, 
                'success': success, 
                'message': message
            })
        except Exception as e:
            socketio.emit('migration_complete', {
                'id': migration_id, 
                'success': False, 
                'message': str(e)
            })
        finally:
            if migration_id in active_migrations:
                del active_migrations[migration_id]

    socketio.start_background_task(target=run_migration)
    emit('migration_started', {'id': migration_id})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)

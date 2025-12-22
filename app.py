from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import storage
import migration
import threading
import os
import secrets

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(16)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Keep track of active migrations
active_migrations = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/connections', methods=['GET'])
def get_connections():
    return jsonify(storage.get_connections_grouped())

@app.route('/api/connections', methods=['POST'])
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
def delete_connection(conn_id):
    storage.delete_connection(conn_id)
    return jsonify({'success': True})

@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    data = request.json
    success, message = migration.test_connection(data['uri'])
    return jsonify({'success': success, 'message': message})

@app.route('/api/db-stats', methods=['POST'])
def get_db_stats():
    data = request.json
    try:
        stats = migration.get_db_stats(data['uri'], data['dbname'])
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/preflight', methods=['POST'])
def preflight():
    data = request.json
    checks = migration.preflight_check(data['source'], data['target'])
    return jsonify({'checks': checks})

@socketio.on('start_migration')
def handle_migration(data):
    migration_id = secrets.token_hex(4)
    source = data['source']
    target = data['target']
    
    def log_callback(message):
        socketio.emit('migration_log', {'id': migration_id, 'message': message})
    
    def run_migration():
        success, message = migration.migrate_db(source, target, log_callback)
        socketio.emit('migration_complete', {
            'id': migration_id, 
            'success': success, 
            'message': message
        })
        if migration_id in active_migrations:
            del active_migrations[migration_id]

    thread = threading.Thread(target=run_migration)
    active_migrations[migration_id] = thread
    thread.start()
    
    emit('migration_started', {'id': migration_id})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)

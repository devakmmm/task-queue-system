# api_server.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from task_queue_system import (
    TaskManager, TaskPriority,
    add_numbers, multiply_numbers, simulate_work, process_data
)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Global task manager instance (set in __main__)
task_manager = None


def init_task_manager():
    """Initialize the task manager with registered functions"""
    global task_manager
    tm = TaskManager(max_workers=4)
    tm.register_task("add", add_numbers)
    tm.register_task("multiply", multiply_numbers)
    tm.register_task("simulate_work", simulate_work)
    tm.register_task("process_data", process_data)
    tm.start()
    task_manager = tm
    print("Task manager initialized and started")


def tm_or_503():
    """Return task_manager or a (response, code) tuple if not ready"""
    if task_manager is None:
        return None, (jsonify({"error": "TASK_MANAGER not initialized"}), 503)
    return task_manager, None


@app.get('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'task-queue-api'
    })


@app.post('/tasks')
def submit_task():
    tm, err = tm_or_503()
    if err:
        return err
    try:
        data = request.get_json()
        if not data or 'func_name' not in data:
            return jsonify({'error': 'func_name is required'}), 400

        func_name = data['func_name']
        args = data.get('args', [])
        kwargs = data.get('kwargs', {})
        priority = data.get('priority', 'NORMAL')
        max_retries = data.get('max_retries', 3)

        try:
            priority_enum = TaskPriority[priority.upper()]
        except KeyError:
            return jsonify({
                'error': f'Invalid priority. Must be one of: {[p.name for p in TaskPriority]}'
            }), 400

        # NOTE: pass args/kwargs by name to avoid "multiple values for func_name"
        task_id = tm.submit_task(
            func_name=func_name,
            args=args,
            kwargs=kwargs,
            priority=priority_enum,
            max_retries=max_retries,
        )

        return jsonify({'task_id': task_id, 'status': 'submitted'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/tasks/<task_id>')
def get_task_status(task_id):
    tm, err = tm_or_503()
    if err:
        return err
    try:
        status = tm.get_task_status(task_id)
        if status is None:
            return jsonify({'error': 'Task not found'}), 404
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/tasks')
def get_all_tasks():
    tm, err = tm_or_503()
    if err:
        return err
    try:
        status_filter = request.args.get('status')
        limit = request.args.get('limit', type=int)

        tasks = tm.get_all_tasks()
        if status_filter:
            tasks = [t for t in tasks if t['status'] == status_filter.lower()]
        if limit:
            tasks = tasks[:limit]

        return jsonify({'tasks': tasks, 'count': len(tasks)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/stats')
def get_system_stats():
    tm, err = tm_or_503()
    if err:
        return err
    try:
        return jsonify(tm.get_stats())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.get('/tasks/types')
def get_available_task_types():
    tm, err = tm_or_503()
    if err:
        return err
    try:
        types_ = tm.task_registry.list_tasks()
        return jsonify({'available_tasks': types_, 'count': len(types_)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.post('/tasks/bulk')
def submit_bulk_tasks():
    tm, err = tm_or_503()
    if err:
        return err
    try:
        data = request.get_json()
        if not data or 'tasks' not in data:
            return jsonify({'error': 'tasks array is required'}), 400

        submitted, errors = [], []
        for i, t in enumerate(data['tasks']):
            try:
                func_name = t.get('func_name')
                if not func_name:
                    errors.append(f'Task {i}: func_name is required')
                    continue

                args = t.get('args', [])
                kwargs = t.get('kwargs', {})
                priority = t.get('priority', 'NORMAL')
                max_retries = t.get('max_retries', 3)

                try:
                    priority_enum = TaskPriority[priority.upper()]
                except KeyError:
                    errors.append(f'Task {i}: Invalid priority {priority}')
                    continue

                # NOTE: named arguments here, too
                tid = tm.submit_task(
                    func_name=func_name,
                    args=args,
                    kwargs=kwargs,
                    priority=priority_enum,
                    max_retries=max_retries,
                )
                submitted.append({'task_id': tid, 'index': i, 'func_name': func_name})

            except Exception as e:
                errors.append(f'Task {i}: {str(e)}')

        return jsonify({
            'submitted_tasks': submitted,
            'submitted_count': len(submitted),
            'errors': errors,
            'error_count': len(errors)
        }), (201 if submitted else 400)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(_):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({'error': 'Method not allowed'}), 405


@app.errorhandler(500)
def internal_error(_):
    return jsonify({'error': 'Internal server error'}), 500


@app.get('/examples')
def get_examples():
    return jsonify({
        'submit_simple_task': {
            'method': 'POST',
            'url': '/tasks',
            'body': {'func_name': 'add', 'args': [10, 20]}
        },
        'submit_priority_task': {
            'method': 'POST',
            'url': '/tasks',
            'body': {'func_name': 'multiply', 'args': [5, 6], 'priority': 'HIGH', 'max_retries': 5}
        },
        'submit_task_with_kwargs': {
            'method': 'POST',
            'url': '/tasks',
            'body': {'func_name': 'simulate_work', 'kwargs': {'duration': 3, 'fail': False}}
        },
        'submit_bulk_tasks': {
            'method': 'POST',
            'url': '/tasks/bulk',
            'body': {'tasks': [
                {'func_name': 'add', 'args': [1, 2]},
                {'func_name': 'multiply', 'args': [3, 4], 'priority': 'HIGH'}
            ]}
        },
        'get_task_status': {'method': 'GET', 'url': '/tasks/{task_id}'},
        'get_all_tasks': {'method': 'GET', 'url': '/tasks?status=completed&limit=10'},
        'get_stats': {'method': 'GET', 'url': '/stats'}
    })


# ... keep everything else the same above
if __name__ == '__main__':
    import os
    PORT = int(os.getenv("PORT", "5050"))  # <— use $PORT if provided

    print(f"Server starting on http://localhost:{PORT}")
    init_task_manager()
    try:
        app.run(debug=True, host='0.0.0.0', port=PORT, use_reloader=False, threaded=True)
    finally:
        if task_manager:
            task_manager.stop()
            print("Task manager stopped")

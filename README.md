# VeloxAPI

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/Python-3.8+-brightgreen.svg)](https://www.python.org/)
[![Database](https://img.shields.io/badge/Database-PostgreSQL-336791.svg)](https://www.postgresql.org/)
[![Framework](https://img.shields.io/badge/Framework-Flask-black.svg)](https://flask.palletsprojects.com/)

**VeloxAPI** is a high-performance, database-backed background task queue, execution engine, and monitoring dashboard designed for **Flask** web applications.

Unlike heavy multi-service queues that require external brokers like Redis or RabbitMQ, VeloxAPI uses your existing **PostgreSQL** database with row-level locking (`FOR UPDATE SKIP LOCKED`) and monotonic **UUIDv7** identifiers. It provides bulletproof persistence, zero data loss, schema isolation, automatic logging capture, and an out-of-the-box management dashboard.

- **Database-Backed Queue**: Tasks are safely written to a PostgreSQL table and processed by background worker threads.
- **Flask Integration**: Easily expose normal functions as asynchronous HTTP endpoints.
- **Embedded Web Dashboard**: View and monitor task statistics, processing states, parameters, execution trace logs, and download zip files of execution archives.
- **Clipboard Integration**: Fast copy buttons for parameters, results, and debug errors.

---

## ⚡ Highlights

- **Brokerless Architecture**: Runs entirely on PostgreSQL. No Redis, Celery, or RabbitMQ required.
- **High-Concurrency Safety**: Uses PostgreSQL `FOR UPDATE SKIP LOCKED` for lock-free, race-free worker job distribution.
- **Monotonic UUIDv7 IDs**: Millisecond time-ordered UUIDv7 identifiers for high-efficiency B-tree indexing and timeline collation.
- **Seamless Flask Integration**: Easily expose functions as asynchronous HTTP endpoints via the `@velox_api.async_task` decorator.
- **Dual Execution Modes**: Dispatch asynchronously (returns `202 Accepted` with a tracking receipt) or execute synchronously on demand with error tracking.
- **Thread-Safe Log Interception**: Automatically captures standard Python `logging` output emitted during task execution and stores it alongside task records.
- **Crash Recovery & Self-Healing**: Resets uncompleted/stuck tasks back to the queue upon application startup.
- **Multi-Tenant Schema Isolation**: Run multiple isolated services or microservice queues on the same database by assigning distinct PostgreSQL schemas.
- **Built-in Web Dashboard**: Includes a responsive UI with status filtering, date range and text search, paginated ledger, clipboard helpers, and `.zip` archive diagnostics downloads.
- **Automated Retention Pruning**: Configurable `resultTTL` automatically cleans up aged tasks.

---

## 📦 Installation

Install VeloxAPI in your Python environment:

```bash
pip install velox
```

Or install in editable mode during development:

```bash
pip install -e /path/to/velox-api
```

### Requirements
- Python `>= 3.8`
- Flask `>= 2.0.0`
- psycopg2-binary `>= 2.9.0`
- PostgreSQL `>= 9.5` (supports `SKIP LOCKED`)

---

## 🚀 Quick Start

Here is a minimal, complete example showing how to initialize VeloxAPI, register async tasks, mount the dashboard, and start background workers:

```python
import logging
from flask import Flask
from velox import VeloxAPI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 1. Initialize VeloxAPI with database credentials
velox_api = VeloxAPI(
    schema="billing_service",
    host="localhost",
    port=5432,
    user="postgres",
    password="your_password",
    dbname="services_db",
    dashboard_name="Billing Task Monitor",
    resultTTL="7 days"
)

# 2. Define and decorate background tasks
@velox_api.async_task(priority=10, route="/api/generate-invoice", methods=["POST"])
def generate_invoice(user_id, amount, currency="USD", **kwargs):
    logger.info(f"Generating invoice for user {user_id} ({amount} {currency})...")
    
    # Perform intensive task processing here...
    invoice_number = f"INV-{user_id}-9942"
    
    logger.info("Invoice created successfully.")
    return {
        "invoice_number": invoice_number,
        "status": "paid",
        "user_id": user_id,
        "amount": amount
    }

# 3. Mount the Velox blueprint to your Flask application
app.register_blueprint(velox_api.blueprint, url_prefix="/tasks")

if __name__ == "__main__":
    # 4. Start worker threads (e.g., 4 concurrent workers)
    velox_api.start_workers(num_threads=4)

    # 5. Start Flask development server
    app.run(port=5000, debug=True)
```

Visit the dashboard in your browser at:
```
http://localhost:5000/tasks/dashboard
```

---

## ⚙️ Configuration Reference

### `VeloxAPI` Initialization Parameters

```python
velox_api = VeloxAPI(
    db_uri=None,
    min_conn=1,
    max_conn=10,
    schema="public",
    dashboard_name="Service Dashboard",
    resultTTL=None,
    **db_kwargs
)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `db_uri` | `str` | `None` | Full PostgreSQL connection string (e.g. `postgresql://user:pass@localhost:5432/dbname`). |
| `**db_kwargs` | `kwargs` | `{}` | Direct database connection arguments passed to `psycopg2` (`host`, `port`, `user`, `password`, `dbname`, etc.). |
| `schema` | `str` | `"public"` | Target PostgreSQL schema for isolation. *Note: `"dashboard"` is reserved.* |
| `min_conn` | `int` | `1` | Minimum database connection pool size. |
| `max_conn` | `int` | `10` | Maximum database connection pool size. |
| `dashboard_name` | `str` | `"Service Dashboard"` | Title displayed on the header of the monitoring dashboard. |
| `resultTTL` | `str` | `None` | Task retention duration (e.g., `'24 hours'`, `'7 days'`, `'30 days'`). Completed and failed tasks older than this interval are automatically deleted. |

---

## 🛠️ Task Registration & Execution

### The `@velox_api.async_task` Decorator

```python
@velox_api.async_task(
    priority=0,
    route=None,
    methods=['POST'],
    sync=False
)
def my_task(**kwargs):
    ...
```

| Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `priority` | `int` | `0` | Default execution priority. Higher numbers are picked up first by workers. |
| `route` | `str` | `None` | Custom URL route. Defaults to `/<function_name>`. |
| `methods` | `list[str]` | `['POST']` | Supported HTTP methods for the route. |
| `sync` | `bool` | `False` | When `True`, route executes synchronously on the request thread by default. |

---

### Calling Tasks

Tasks can be triggered either via HTTP or programmatically in Python code:

#### 1. Asynchronous HTTP Request (Default)
Send an HTTP request to the endpoint:

```bash
curl -X POST http://localhost:5000/tasks/api/generate-invoice \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "amount": 199.99, "currency": "USD"}'
```

**Response (`202 Accepted`):**
```json
{
  "message": "Task 'generate_invoice' successfully received and queued.",
  "receipt": {
    "status": "queued",
    "task_id": "018d9f4e-28b4-7b90-8ef5-08e5c1a7d65b"
  }
}
```

#### 2. Synchronous Execution Override
You can force synchronous execution on any request by setting `?sync=true` in the query parameters or `{"sync": true}` in the JSON payload:

```bash
curl -X POST "http://localhost:5000/tasks/api/generate-invoice?sync=true" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "amount": 199.99}'
```

**Response (`200 OK` on success):**
```json
{
  "invoice_number": "INV-42-9942",
  "status": "paid",
  "user_id": 42,
  "amount": 199.99
}
```

**Response (`500 Internal Server Error` on failure):**
```json
{
  "error": "Database connection timeout",
  "task_id": "018d9f4e-28b4-7b90-8ef5-08e5c1a7d65b"
}
```

#### 3. Programmatic Enqueueing from Python
Decorated functions can be invoked directly anywhere in your Python codebase to enqueue a task into the database queue:

```python
# Direct Python call queues the task and returns a receipt dictionary
receipt = generate_invoice(user_id=101, amount=49.99, currency="USD")
print(receipt)
# Output: {'task_id': '018d9f4e-28b4-7b90-8ef5-08e5c1a7d65b', 'status': 'queued'}
```

---

## 🪵 Automatic Log Capture

VeloxAPI integrates a custom thread-local logging handler (`VeloxTaskLogHandler`). Any standard logging calls made inside a task are captured and persisted with the task record:

```python
import logging

logger = logging.getLogger(__name__)

@velox_api.async_task()
def process_data(dataset_id, **kwargs):
    logger.info(f"Starting processing for dataset {dataset_id}")
    logger.debug("Parsing records...")
    logger.warning("Found 2 unformatted lines, skipping...")
    logger.info("Processing complete.")
    return {"processed": True}
```

Captured logs can be viewed in real-time in the dashboard modal or fetched via the `/dashboard/<task_id>/logs` API endpoint.

---

## 📊 Dashboard & Diagnostics Endpoints

When you mount `velox_api.blueprint`, the following endpoints are automatically available:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/dashboard` | `GET` | Interactive web dashboard UI. Supports `?status=`, `?search=`, `?min_date=`, `?max_date=`, and `?page=`. |
| `/dashboard/<task_id>` | `GET` | Returns full JSON ledger of the task (status, result, error, logs). |
| `/dashboard/<task_id>/args` | `GET` | Returns the input payload arguments passed to the task. |
| `/dashboard/<task_id>/result` | `GET` | Returns the output result returned by the task function. |
| `/dashboard/<task_id>/error` | `GET` | Returns the recorded traceback if the task encountered an exception. |
| `/dashboard/<task_id>/logs` | `GET` | Returns standard logging emitted during task execution. |
| `/dashboard/<task_id>/archive` | `GET` | Downloads a `.zip` archive containing `payload.json`, `result.json`, `error.log`, and `task.log`. |

---

## 🏗️ Architecture & Database Design

### Schema Isolation & Table Layout
Velox automatically creates and manages its table layout inside the configured PostgreSQL schema (`{schema}.tasks`):

```sql
CREATE TABLE IF NOT EXISTS {schema}.tasks (
    id TEXT PRIMARY KEY,               -- UUIDv7 (monotonic, millisecond-indexed)
    task_name TEXT NOT NULL,           -- Registered Python function name
    args JSONB NOT NULL,               -- Merged JSON input payload
    status TEXT NOT NULL DEFAULT 'queued', -- queued | processing | completed | failed
    priority INT NOT NULL DEFAULT 0,   -- Scheduling priority (higher executes first)
    result JSONB,                      -- Output payload on successful completion
    error_message TEXT,                -- Full exception traceback on failure
    logs TEXT,                         -- Captured thread-local logging output
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_qp_{schema} 
ON {schema}.tasks (priority DESC, created_at ASC) 
WHERE status = 'queued';
```

### Concurrency & Locking
Worker threads claim tasks using PostgreSQL row locking with `SKIP LOCKED`:

```sql
UPDATE {schema}.tasks
SET status = 'processing', started_at = NOW()
WHERE id = (
    SELECT id FROM {schema}.tasks
    WHERE status = 'queued'
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, task_name, args;
```
This guarantees that multiple concurrent workers—even across separate server processes or container instances—never compete or double-process the same task.

---

## 🚀 Production Deployment

### Running with Gunicorn / uWSGI
When deploying Flask with multi-process WSGI servers (like Gunicorn or uWSGI), start the Velox worker threads within the worker initialization hooks or start worker processes separately:

```python
# wsgi.py
from my_app import create_app, velox_api

app = create_app()

# Start worker threads per process or conditionally on master
velox_api.start_workers(num_threads=4)
```

Ensure your PostgreSQL `max_connections` and Velox `max_conn` pool sizes are configured to handle the total number of processes:

```text
Total Connections = WSGI Processes × max_conn
```

---

## 📄 License

VeloxAPI is open-source software licensed under the [MIT License](LICENSE).

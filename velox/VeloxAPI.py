import json
import os
import time
import threading
import io
import zipfile
import uuid
import logging
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from flask import Blueprint, jsonify, abort, request, send_file

# Set up module-level logging
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)

# Global registry to map task names to Python functions
TASK_REGISTRY = {}

def generate_task_id():
    """
    Generates a time-based UUID following the UUIDv7 layout.
    
    Combines a millisecond timestamp with random bytes, applying the correct
    RFC 4122 version and variant bits. This guarantees monotonic time sorting
    and avoids collisions under high concurrency.
    
    Returns:
        str: A string representation of the generated UUID.
    """
    milli = int(time.time() * 1000)
    ts_bytes = milli.to_bytes(6, byteorder='big')
    rand_bytes = os.urandom(10)
    
    # Set version 7 high bits (0x70)
    v_high = (rand_bytes[0] & 0x0f) | 0x70
    v_low = rand_bytes[1]
    
    # Set variant 2 bits (RFC 4122)
    var_high = (rand_bytes[2] & 0x3f) | 0x80
    
    uuid_bytes = bytes([
        ts_bytes[0], ts_bytes[1], ts_bytes[2], ts_bytes[3], ts_bytes[4], ts_bytes[5],
        v_high, v_low,
        var_high, rand_bytes[3], rand_bytes[4], rand_bytes[5], rand_bytes[6], rand_bytes[7], rand_bytes[8], rand_bytes[9]
    ])
    return str(uuid.UUID(bytes=uuid_bytes))

# Thread-local storage context to isolate task logging state
_thread_local = threading.local()

class VeloxTaskLogHandler(logging.Handler):
    """
    A custom logging handler that intercepts messages emitted by the root logger
    and routes them to a thread-local task log storage block.
    
    Only records messages when a current task context is active on the current thread.
    """
    def __init__(self):
        super().__init__()
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    def emit(self, record):
        task_id = getattr(_thread_local, 'current_task_id', None)
        if task_id is not None:
            try:
                log_entry = self.format(record)
                if not hasattr(_thread_local, 'task_logs'):
                    _thread_local.task_logs = []
                _thread_local.task_logs.append(log_entry)
            except Exception:
                self.handleError(record)

class VeloxAPI:
    """
    Core API service manager for the Velox task orchestration engine.
    
    Initializes database pools, bootstraps queue schemas, starts worker loops,
    and automatically binds Flask blueprint routes for dashboard monitoring.
    """
    def __init__(self, db_uri=None, min_conn=1, max_conn=10, schema="public", dashboard_name="Service Dashboard", resultTTL=None, **db_kwargs):
        """
        Initializes the VeloxAPI instance.
        
        Args:
            db_uri (str, optional): Connection string for the PostgreSQL database.
            min_conn (int): Minimum database connection pool size.
            max_conn (int): Maximum database connection pool size.
            schema (str): PostgreSQL schema namespace where queue tables reside.
            dashboard_name (str): Display name for the HTML monitoring UI.
            resultTTL (str, optional): ISO 8601 duration string for task expiration cleanup.
            **db_kwargs: Additional raw parameters passed to psycopg2 pool.
            
        Raises:
            ValueError: If 'dashboard' is selected as schema, or database connection details are missing.
        """
        if schema.lower().strip('/') == 'dashboard':
            raise ValueError(
                "VeloxAPI Configuration Error: 'dashboard' is a reserved keyword utility "
                "used by VeloxAPI for the monitoring UI. Please choose a different name "
                "for your service schema (e.g., 'weather_service', 'data_layer')."
            )

        self.schema = schema
        self.routes = {}
        self.dashboard_name = dashboard_name
        self.resultTTL = resultTTL

        # Register thread-local log handler to capture task logging output
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        if not any(isinstance(h, VeloxTaskLogHandler) for h in root_logger.handlers):
            root_logger.addHandler(VeloxTaskLogHandler())

        # Establish connection pool using either DB URI or raw credentials kwargs
        if db_uri:
            self.pool = ThreadedConnectionPool(min_conn, max_conn, dsn=db_uri)
        elif db_kwargs:
            self.pool = ThreadedConnectionPool(min_conn, max_conn, **db_kwargs)
        else:
            raise ValueError("You must provide either a 'db_uri' string OR database connection keywords.")
        
        self._bootstrap_database()
        self.blueprint = Blueprint('velox_api_core', __name__)

        # Register Flask before_request hook for automatic result TTL cleanup
        @self.blueprint.before_request
        def before_request_cleanup():
            self._prune_expired_tasks()

        self._register_routes()

    def _bootstrap_database(self):
        """
        Creates the tables, schemas, and indices necessary to support the Velox task queue.
        
        Runs migrations dynamically (e.g. adding the logs text column if updating
        an older installation). Uses a standalone connection transaction.
        """
        logger.info('Bootstrapping VeloxAPI database schema and tables...')
        conn = self.pool.getconn()
        try:
            conn.autocommit = False
            
            # PHASE 1: Create and isolate the schema namespace
            with conn.cursor() as cursor:
                logger.info(f"Ensuring schema '{self.schema}' exists...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            conn.commit()

            # PHASE 2: Verify the table layout exists
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = 'tasks'
                    );
                """, (self.schema,))
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    logger.info(f"Creating 'tasks' table within '{self.schema}'...")
                    cursor.execute(f"""
                        CREATE TABLE {self.schema}.tasks (
                            id TEXT PRIMARY KEY,
                            task_name TEXT NOT NULL,
                            args JSONB NOT NULL,
                            status TEXT NOT NULL DEFAULT 'queued',
                            priority INT NOT NULL DEFAULT 0,
                            result JSONB,
                            error_message TEXT,
                            logs TEXT,
                            created_at TIMESTAMP DEFAULT NOW(),
                            started_at TIMESTAMP,
                            completed_at TIMESTAMP
                        );
                    """)
                    
                    # Ensure a uniquely named index per-schema to avoid global conflicts
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_qp_{self.schema} 
                        ON {self.schema}.tasks (priority DESC, created_at ASC) 
                        WHERE status = 'queued';
                    """)
            conn.commit()

            # PHASE 3: Ensure logs column exists on tasks table for backward compatibility migration
            with conn.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {self.schema}.tasks ADD COLUMN IF NOT EXISTS logs TEXT;")
            conn.commit()

            # PHASE 4: Recover stuck processing tasks (Approach B: reset to queued on startup)
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {self.schema}.tasks
                    SET status = 'queued', started_at = NULL
                    WHERE status = 'processing';
                """)
                row_count = cursor.rowcount
                if row_count > 0:
                    logger.info(f"Recovered {row_count} tasks stuck in 'processing' status, reset to 'queued'.")
            conn.commit()

            logger.info("VeloxAPI database bootstrap successfully locked to disk.")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to bootstrap VeloxAPI database: {e}")
            raise e
        finally:
            self.pool.putconn(conn)

    def _prune_expired_tasks(self):
        """
        Queries the database and deletes all completed/failed tasks older than the resultTTL.
        
        Calculates intervals natively in PostgreSQL. If self.resultTTL is None, 
        performs no operations.
        """
        if not self.resultTTL:
            return
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    DELETE FROM {self.schema}.tasks
                    WHERE status IN ('completed', 'failed')
                      AND (completed_at < NOW() - CAST(%s AS INTERVAL) OR completed_at IS NULL);
                """, (self.resultTTL,))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to prune expired tasks: {e}")
        finally:
            self.return_conn(conn)

    def _register_routes(self):
        """Binds the REST endpoints to the blueprint automatically."""

        @self.blueprint.route('/dashboard', methods=['GET'])
        def dashboard():
            """
            Renders the main service monitoring dashboard.
            
            Loads and populates task statistics, status filters, and execution listings
            from the database tasks. Supports pagination, text search, and date range filters.
            """
            # 1. Parse query parameter filters
            target_status = request.args.get('status', 'all')
            search = request.args.get('search', '').strip()
            min_date = request.args.get('min_date', '').strip()
            max_date = request.args.get('max_date', '').strip()
            page = request.args.get('page', 1)
            try:
                page = int(page)
                if page < 1:
                    page = 1
            except ValueError:
                page = 1

            PAGE_SIZE = 100

            # 2. Build dynamic PostgreSQL WHERE clause and query parameters
            where_clauses = []
            query_params = []

            if target_status != 'all':
                where_clauses.append("status = %s")
                query_params.append(target_status)

            if search:
                where_clauses.append("(task_name ILIKE %s OR id ILIKE %s)")
                search_like = f"%{search}%"
                query_params.append(search_like)
                query_params.append(search_like)

            if min_date:
                where_clauses.append("created_at >= %s")
                query_params.append(min_date)

            if max_date:
                where_clauses.append("created_at <= %s")
                query_params.append(max_date)

            where_str = ""
            if where_clauses:
                where_str = "WHERE " + " AND ".join(where_clauses)

            # Determine sorting order based on status tabs
            if target_status == 'all':
                order_str = "ORDER BY created_at DESC, priority DESC"
            else:
                order_str = "ORDER BY priority DESC, created_at ASC"

            # 3. Query the database for total count and paginated records
            conn = self.get_conn()
            try:
                # 3.1 Fetch total task count matching filters (for pagination limits)
                with conn.cursor() as count_cur:
                    count_cur.execute(f"""
                        SELECT COUNT(*) 
                        FROM {self.schema}.tasks
                        {where_str};
                    """, tuple(query_params))
                    total_count = count_cur.fetchone()[0]

                # 3.2 Bound current page and compute page offset
                import math
                total_pages = max(1, math.ceil(total_count / PAGE_SIZE))
                if page > total_pages:
                    page = total_pages
                offset = (page - 1) * PAGE_SIZE

                # 3.3 Fetch paginated tasks records
                paginated_params = list(query_params)
                paginated_params.extend([PAGE_SIZE, offset])

                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT id, task_name, priority, status, created_at, started_at
                        FROM {self.schema}.tasks
                        {where_str}
                        {order_str}
                        LIMIT %s OFFSET %s;
                    """, tuple(paginated_params))
                    tasks = cursor.fetchall()
            finally:
                self.return_conn(conn)

            # 4. Generate URL helpers preserving active filters
            def get_url_for_status(s):
                parts = [f"status={s}"]
                if search:
                    import urllib.parse
                    parts.append(f"search={urllib.parse.quote(search)}")
                if min_date:
                    parts.append(f"min_date={min_date}")
                if max_date:
                    parts.append(f"max_date={max_date}")
                return "?" + "&".join(parts)

            def get_page_url(p):
                parts = []
                if target_status != 'all':
                    parts.append(f"status={target_status}")
                if search:
                    import urllib.parse
                    parts.append(f"search={urllib.parse.quote(search)}")
                if min_date:
                    parts.append(f"min_date={min_date}")
                if max_date:
                    parts.append(f"max_date={max_date}")
                parts.append(f"page={p}")
                return "?" + "&".join(parts)

            # 5. Format status navigation tabs HTML
            status_tabs = "".join([
                f'<a href="{get_url_for_status(s)}" class="tab {"active" if target_status == s else ""}">{s.upper()}</a>'
                for s in ['all', 'queued', 'processing', 'completed', 'failed']
            ])

            # 6. Format pagination controls HTML
            start_num = offset + 1 if total_count > 0 else 0
            end_num = min(offset + PAGE_SIZE, total_count)
            pagination_info = f"Showing {start_num} to {end_num} of {total_count} tasks"

            if page > 1:
                prev_btn = f'<a href="{get_page_url(page - 1)}" class="btn-page">Previous</a>'
            else:
                prev_btn = '<span class="btn-page disabled">Previous</span>'

            if page < total_pages:
                next_btn = f'<a href="{get_page_url(page + 1)}" class="btn-page">Next</a>'
            else:
                next_btn = '<span class="btn-page disabled">Next</span>'

            pagination_controls = f"""
                <span class="pagination-info">{pagination_info}</span>
                <div class="pagination-buttons">
                    {prev_btn}
                    {next_btn}
                </div>
            """

            # 7. Setup Filter Bar Clear and active states
            clear_search_btn = ""
            filter_active_class = ""
            if search or min_date or max_date:
                filter_active_class = "active"
                clear_search_btn = f'<a href="?status={target_status}" class="btn-search" style="text-decoration: none; display: flex; align-items: center; justify-content: center; background: #ffeef0; color: #d73a49; border-color: #f9d0d4;">Clear</a>'

            # 8. Render task row templates
            table_rows = ""
            for task in tasks:
                created_utc = task['created_at'].isoformat() if task['created_at'] else '-'
                started_utc = task['started_at'].isoformat() if task['started_at'] else '-'

                # Build row-specific action ellipsis dropdown options
                param_action_cell = f"""
                <div class="dropdown">
                    <button class="dropdown-trigger" onclick="toggleDropdown(event, '{task['id']}')">&#8942;</button>
                    <div id="dropdown-menu-{task['id']}" class="dropdown-menu">
                        <button onclick="toggleDetails('{task['id']}', 'payload')">Payload</button>
                """

                if task['status'] == 'completed':
                    param_action_cell += f"""
                        <button onclick="toggleDetails('{task['id']}', 'result')">Result</button>
                    """
                elif task['status'] == 'failed':
                    param_action_cell += f"""
                        <button onclick="toggleDetails('{task['id']}', 'error')">Error</button>
                    """

                if task['status'] in ('completed', 'failed'):
                    param_action_cell += f"""
                        <button onclick="toggleDetails('{task['id']}', 'logs')">Logs</button>
                    """

                # Add ZIP archive download action
                param_action_cell += f"""
                        <a href="./dashboard/{task['id']}/archive">Download Zip</a>
                    </div>
                </div>
                """

                short_id = task['id'][-8:]
                table_rows += f"""
                <tr class="row-{task['status']}">
                    <td><b class="id-clickable" title="Click to toggle full ID" onclick="toggleSingleId(this)"><span class="id-short">{short_id}</span><span class="id-full">{task['id']}</span></b></td>
                    <td><code class="task-name">{task['task_name']}</code></td>
                    <td><span class="badge {task['status']}">{task['status']}</span></td>
                    <td><span class="priority-num">{task['priority']}</span></td>
                    <td class="local-time" data-utc="{created_utc}">{created_utc}</td>
                    <td class="local-time" data-utc="{started_utc}">{started_utc}</td>
                    <td style="text-align: right; padding-right: 20px;">{param_action_cell}</td>
                </tr>
                """

            if not table_rows:
                table_rows = f'<tr><td colspan="7" class="empty-state">No tasks found under current filter selection.</td></tr>'

            # 9. Read the HTML layout template and format it
            template_path = os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html')
            with open(template_path, 'r', encoding='utf-8') as f:
                html_template = f.read()
                
            return html_template.format(
                dashboard_name=self.dashboard_name,
                status_tabs=status_tabs,
                table_rows=table_rows,
                search_val=search,
                min_date_val=min_date,
                max_date_val=max_date,
                target_status=target_status,
                clear_search_btn=clear_search_btn,
                filter_active_class=filter_active_class,
                pagination_controls=pagination_controls
            )

        @self.blueprint.route('/dashboard/<task_id>', methods=['GET'])
        def dashboard_task(task_id):
            """
            Fetches and returns the JSON task ledger containing status, results, and errors.
            """
            connection = self.get_conn()
            try:
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"SELECT id, status, result, error_message, logs FROM {self.schema}.tasks WHERE id = %s;", (task_id,))
                    row = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not row:
                abort(404, description="Task not found")

            res_payload = None
            if row['result']:
                try:
                    res_payload = json.loads(row['result']) if isinstance(row['result'], str) else row['result']
                except Exception:
                    res_payload = row['result']

            return jsonify({
                "task_id": row['id'],
                "status": row['status'],
                "result": res_payload,
                "error": row['error_message'],
                "logs": row['logs']
            })

        @self.blueprint.route('/dashboard/<task_id>/args', methods=['GET'])
        def dashboard_args(task_id):
            """
            Fetches and returns the JSON input arguments payload for a specific task.
            """
            connection = self.get_conn()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT args FROM {self.schema}.tasks WHERE id = %s;", (task_id,))
                    result = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not result:
                abort(404, description="Task not found")

            payload = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            return jsonify(payload)

        @self.blueprint.route('/dashboard/<task_id>/result', methods=['GET'])
        def dashboard_result(task_id):
            """
            Fetches and returns the JSON output result dataset generated by a completed task.
            """
            connection = self.get_conn()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT result FROM {self.schema}.tasks WHERE id = %s;", (task_id,))
                    result = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not result:
                abort(404, description="Task not found")

            res_payload = json.loads(result[0]) if isinstance(result[0], str) else result[0]
            return jsonify(res_payload if res_payload else {"message": "No output payload generated for this row context."})

        @self.blueprint.route('/dashboard/<task_id>/error', methods=['GET'])
        def dashboard_error(task_id):
            """
            Fetches and returns the recorded execution traceback error message for a failed task.
            """
            connection = self.get_conn()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT error_message FROM {self.schema}.tasks WHERE id = %s;", (task_id,))
                    result = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not result:
                abort(404, description="Task not found")

            return jsonify({"error": result[0] if result[0] else "No traceback error messages recorded for this execution row."})

        @self.blueprint.route('/dashboard/<task_id>/logs', methods=['GET'])
        def dashboard_logs(task_id):
            """
            Fetches and returns the captured runtime execution logs for a specific task.
            """
            connection = self.get_conn()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT logs FROM {self.schema}.tasks WHERE id = %s;", (task_id,))
                    result = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not result:
                abort(404, description="Task not found")

            return jsonify({"logs": result[0] if result[0] else ""})

        @self.blueprint.route('/dashboard/<task_id>/archive', methods=['GET'])
        def dashboard_archive(task_id):
            """
            Compiles a task's arguments, results, tracebacks, and execution logs
            into a ZIP archive and triggers a browser download attachment.
            """
            connection = self.get_conn()
            try:
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT task_name, args, result, error_message, logs 
                        FROM {self.schema}.tasks 
                        WHERE id = %s;
                    """, (task_id,))
                    row = cursor.fetchone()
            finally:
                self.return_conn(connection)

            if not row:
                abort(404, description="Requested execution ledger instance missing.")

            # Compile zip payload file assets in memory buffer
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                payload_obj = json.loads(row['args']) if isinstance(row['args'], str) else row['args']
                zip_file.writestr("payload.json", json.dumps(payload_obj, indent=4))

                if row['result']:
                    result_obj = json.loads(row['result']) if isinstance(row['result'], str) else row['result']
                    zip_file.writestr("result.json", json.dumps(result_obj, indent=4))
                else:
                    zip_file.writestr("result.json", json.dumps({"message": "No result dataset written. Status was not marked completed."}, indent=4))

                if row['error_message']:
                    zip_file.writestr("error.log", row['error_message'])
                else:
                    zip_file.writestr("error.log", f"Clean Execution. Task name: {row['task_name']}")

                if row['logs']:
                    zip_file.writestr("task.log", row['logs'])
                else:
                    zip_file.writestr("task.log", "No logs recorded for this task execution.")

            zip_buffer.seek(0)
            archive_filename = f"velox_task_{task_id}_{row['task_name']}.zip"
            return send_file(
                zip_buffer,
                mimetype="application/zip",
                as_attachment=True,
                download_name=archive_filename
            )

    def get_conn(self):
        """
        Retrieves a database connection from the connection pool.
        
        Returns:
            psycopg2.extensions.connection: A connection object.
        """
        return self.pool.getconn()
        
    def return_conn(self, conn):
        """
        Puts a database connection back into the connection pool.
        
        Args:
            conn (psycopg2.extensions.connection): The connection to return.
        """
        self.pool.putconn(conn)

    def enqueue(self, task_name, payload=None, priority=0):
        """
        Inserts a queued task row for a worker on any instance to claim.

        Args:
            task_name (str): Registered task name, matching the function's __name__.
            payload (dict, optional): Keyword arguments to call the task with.
            priority (int): Scheduling priority; higher is claimed sooner.

        Returns:
            dict: A receipt of the form {"task_id": str, "status": "queued"}.
        """
        serialized_payload = json.dumps(payload or {})
        task_id = generate_task_id()

        connection = self.get_conn()
        try:
            connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.tasks (id, task_name, args, priority)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (task_id, task_name, serialized_payload, priority)
                )
                task_id = cursor.fetchone()[0]
            connection.commit()
            return {"task_id": task_id, "status": "queued"}
        except Exception as db_err:
            connection.rollback()
            logger.error(f"VeloxAPI Queue Insertion Fail: {db_err}")
            raise db_err
        finally:
            self.return_conn(connection)

    def async_task(self, priority=0, route=None, methods=['POST'], sync=False):
        """
        Decorator that registers a Python function as a queueable, monitorable task.
        
        Binds a REST API route to the blueprint allowing clients to trigger the task.
        
        Args:
            priority (int): Default priority level for task scheduling.
            route (str, optional): Custom endpoint path. Defaults to /<task_name>.
            methods (list): Supported HTTP methods. Defaults to ['POST'].
            sync (bool): Set True to execute task synchronously by default.
        """
        def decorator(func):
            task_name = func.__name__
            TASK_REGISTRY[task_name] = func

            def automatic_route_handler(*args, **kwargs):
                # Extract payload from JSON body first (if not GET)
                payload = {}
                if request.method != 'GET':
                    payload = request.get_json(silent=True) or {}
                
                # Merge URL path variables into payload
                payload.update(kwargs)
                
                # Extract sync flag from query parameter or JSON body
                sync_param = request.args.get("sync")
                sync_body = payload.get("sync")
                
                is_sync = sync  # Start with decorator default
                for s in (sync_param, sync_body):
                    if s is not None:
                        if isinstance(s, bool):
                            is_sync = s
                        elif isinstance(s, str):
                            is_sync = s.lower() in ("true", "1", "yes")
                        elif isinstance(s, int):
                            is_sync = bool(s)
                
                # Remove "sync" from query params and payload to avoid passing it to func
                payload.pop("sync", None)
                
                # Merge query parameters into payload (excluding "sync")
                for k, v in request.args.items():
                    if k != "sync":
                        payload[k] = v

                if is_sync:
                    # Synchronous execution path
                    task_id = generate_task_id()
                    serialized_payload = json.dumps(payload)
                    connection = self.get_conn()
                    try:
                        connection.autocommit = False
                        with connection.cursor() as cursor:
                            cursor.execute(
                                f"""
                                INSERT INTO {self.schema}.tasks (id, task_name, args, status, started_at)
                                VALUES (%s, %s, %s, 'processing', NOW())
                                """,
                                (task_id, task_name, serialized_payload)
                            )
                        connection.commit()
                    except Exception as db_err:
                        connection.rollback()
                        raise db_err
                    finally:
                        self.return_conn(connection)

                    # Initialize thread-local logging variables
                    _thread_local.current_task_id = task_id
                    _thread_local.task_logs = []
                    try:
                        result = func(**payload)
                        
                        # Extract data from Flask Response objects for DB logs serialization
                        from flask import Response
                        log_result = result
                        if isinstance(result, Response):
                            log_result = result.get_json()
                        elif isinstance(result, tuple) and len(result) > 0 and isinstance(result[0], Response):
                            log_result = result[0].get_json()
                            
                        logs_str = "\n".join(getattr(_thread_local, 'task_logs', []))
                        self._update_status(task_id, 'completed', result=json.dumps(log_result), logs=logs_str)
                        
                        # Return Flask Response objects directly if returned by decorated function
                        if isinstance(result, Response) or (isinstance(result, tuple) and len(result) > 0 and isinstance(result[0], Response)):
                            return result
                            
                        return jsonify(result), 200
                    except Exception as e:
                        import traceback
                        error_msg = f"{str(e)}\n{traceback.format_exc()}"
                        logs_str = "\n".join(getattr(_thread_local, 'task_logs', []))
                        self._update_status(task_id, 'failed', error=error_msg, logs=logs_str)
                        return jsonify({"error": str(e), "task_id": task_id}), 500
                    finally:
                        _thread_local.current_task_id = None
                        _thread_local.task_logs = []
                else:
                    # Asynchronous execution path (default)
                    receipt = self.enqueue(task_name, payload, priority)
                    return jsonify({
                        "message": f"Task '{task_name}' successfully received and queued.",
                        "receipt": receipt
                    }), 202

            endpoint_url = route if route else f"/{task_name}"
            endpoint_name = task_name
            self.routes[task_name] = endpoint_url
            
            self.blueprint.add_url_rule(
                endpoint_url,
                endpoint_name,
                automatic_route_handler,
                methods=methods
            )

            return func
        return decorator

    def start_workers(self, num_threads=2):
        """
        Spawns background daemon threads that monitor the database tasks queue.
        
        Args:
            num_threads (int): The number of worker threads to start.
        """
        for _ in range(num_threads):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()

    def _worker_loop(self):
        """
        Infinite polling loop executed by background threads.
        
        Claims the next queued task, processes it, and cleans up expired tasks.
        """
        while True:
            connection = self.get_conn()
            task = None
            try:
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Use row-locking with SKIP LOCKED for high-concurrency safety
                    cursor.execute(f"""
                        UPDATE {self.schema}.tasks
                        SET status = 'processing', started_at = NOW()
                        WHERE id = (
                            SELECT id FROM {self.schema}.tasks
                            WHERE status = 'queued'
                            ORDER BY priority DESC, created_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, task_name, args;
                    """)
                    task = cursor.fetchone()
                    connection.commit()
            except Exception as e:
                connection.rollback()
                logger.error(f"Worker DB Error during task claiming: {e}")
            finally:
                self.return_conn(connection)

            if not task:
                time.sleep(1)
                continue

            self._prune_expired_tasks()
            self._execute_task(task)

    def _execute_task(self, task):
        """
        Executes a claimed task, capturing execution logs and updating database status.
        
        Args:
            task (dict): Dictionary containing 'id', 'task_name', and 'args'.
        """
        task_id = task['id']
        func = TASK_REGISTRY.get(task['task_name'])
        raw_args = task['args']
        payload = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
        
        if not func:
            self._update_status(task_id, 'failed', error=f"Function '{task['task_name']}' not registered.")
            return

        # Bind thread-local log state variables
        _thread_local.current_task_id = task_id
        _thread_local.task_logs = []
        try:
            result = func(**payload)
            logs_str = "\n".join(getattr(_thread_local, 'task_logs', []))
            self._update_status(task_id, 'completed', result=json.dumps(result), logs=logs_str)
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logs_str = "\n".join(getattr(_thread_local, 'task_logs', []))
            self._update_status(task_id, 'failed', error=error_msg, logs=logs_str)
        finally:
            _thread_local.current_task_id = None
            _thread_local.task_logs = []

    def _update_status(self, task_id, status, result=None, error=None, logs=None):
        """
        Updates task execution status, results, tracebacks, and captured logs.
        
        Args:
            task_id (str): The UUID of the task.
            status (str): The status state ('completed', 'failed', etc.).
            result (str, optional): JSON string representing task outputs.
            error (str, optional): Stack trace error details.
            logs (str, optional): Captured execution logging messages.
        """
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.schema}.tasks
                    SET status = %s, result = %s, error_message = %s, logs = %s, completed_at = NOW()
                    WHERE id = %s;
                """, (status, result, error, logs, task_id))
                conn.commit()
        except Exception as update_err:
            logger.error(f"Failed to update task status in DB: {update_err}")
        finally:
            self.return_conn(conn)

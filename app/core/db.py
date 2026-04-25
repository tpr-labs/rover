import os
import re
import threading

import oracledb

READ_ONLY_OPS = {"select"}
DEV_WRITE_OPS = {"insert", "update", "delete", "truncate", "drop", "create"}
ALL_ALLOWED_OPS = READ_ONLY_OPS | DEV_WRITE_OPS

_POOL_LOCK = threading.Lock()
_POOL = None


def _create_pool():
    wallet_dir = os.environ.get("ORA_WALLET_DIR", "/tmp/wallet")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_dsn = os.environ.get("DB_DSN", "projectxdev_low")
    db_wallet_password = os.environ.get("DB_WALLET_PASSWORD")

    if not db_user or not db_password or not db_wallet_password:
        raise RuntimeError("Server is not fully configured")

    pool_min = max(1, int(os.environ.get("DB_POOL_MIN", "2")))
    pool_max = max(pool_min, int(os.environ.get("DB_POOL_MAX", "10")))
    pool_inc = max(1, int(os.environ.get("DB_POOL_INC", "1")))

    return oracledb.create_pool(
        user=db_user,
        password=db_password,
        dsn=db_dsn,
        config_dir=wallet_dir,
        wallet_location=wallet_dir,
        wallet_password=db_wallet_password,
        min=pool_min,
        max=pool_max,
        increment=pool_inc,
        getmode=oracledb.SPOOL_ATTRVAL_WAIT,
    )


def get_db_connection():
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                _POOL = _create_pool()
    return _POOL.acquire()


def get_schema() -> str:
    schema = os.environ.get("ORA_SCHEMA", "ADMIN")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", schema):
        raise ValueError("Invalid ORA_SCHEMA format")
    return schema


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql.strip()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_table_name(name: str) -> str:
    value = name.strip().strip('"').lower()
    if "." in value:
        value = value.split(".")[-1]
    return value


def _allowed_tables() -> set[str]:
    raw = os.environ.get("SQL_UI_ALLOWED_TABLES", "")
    tables = {_normalize_table_name(t) for t in raw.split(",") if t.strip()}
    if not tables:
        raise ValueError("SQL_UI_ALLOWED_TABLES must be configured")
    return tables


def _get_operation(sql: str) -> str:
    match = re.match(r"^\s*([a-zA-Z]+)", sql)
    if not match:
        raise ValueError("Unable to determine SQL operation")
    op = match.group(1).lower()
    if op not in ALL_ALLOWED_OPS:
        raise ValueError("Operation is not allowed")
    return op


def _extract_table_names(cleaned_sql: str, operation: str) -> set[str]:
    sql = cleaned_sql.lower()
    names: set[str] = set()

    if operation == "select":
        names.update(re.findall(r"\bfrom\s+([a-zA-Z0-9_\.\"]+)", sql))
        names.update(re.findall(r"\bjoin\s+([a-zA-Z0-9_\.\"]+)", sql))
    elif operation == "insert":
        m = re.search(r"\binsert\s+into\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))
    elif operation == "update":
        m = re.search(r"\bupdate\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))
    elif operation == "delete":
        m = re.search(r"\bdelete\s+from\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))
    elif operation == "truncate":
        m = re.search(r"\btruncate\s+table\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))
    elif operation == "drop":
        m = re.search(r"\bdrop\s+table\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))
    elif operation == "create":
        m = re.search(r"\bcreate\s+table\s+([a-zA-Z0-9_\.\"]+)", sql)
        if m:
            names.add(m.group(1))

    normalized = {_normalize_table_name(n) for n in names if n}
    if not normalized:
        raise ValueError("Could not safely determine table names from query")
    return normalized


def validate_sql_explorer_query(sql: str) -> tuple[str, str]:
    cleaned = _strip_sql_comments(sql)
    if not cleaned:
        raise ValueError("Query cannot be empty")

    if ";" in cleaned:
        raise ValueError("Multiple statements are not allowed")

    operation = _get_operation(cleaned)

    write_enabled = _env_bool("SQL_UI_WRITE_ENABLED", False)
    app_env = os.environ.get("APP_ENV", os.environ.get("FLASK_ENV", "prod")).lower()
    if operation in DEV_WRITE_OPS and not (write_enabled and app_env == "dev"):
        raise ValueError("Write operations are allowed only when APP_ENV=dev and SQL_UI_WRITE_ENABLED=true")

    query_tables = _extract_table_names(cleaned, operation)
    allowed_tables = _allowed_tables()
    if not query_tables.issubset(allowed_tables):
        raise ValueError("Query references tables outside SQL_UI_ALLOWED_TABLES")

    return cleaned, operation


def execute_sql_explorer_query(sql: str, max_rows: int) -> dict:
    safe_sql, operation = validate_sql_explorer_query(sql)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(safe_sql)

            if operation == "select":
                columns = [d[0] for d in (cur.description or [])]
                fetched = cur.fetchmany(max_rows)
                rows = [{columns[i]: row[i] for i in range(len(columns))} for row in fetched]
                return {
                    "operation": operation,
                    "columns": columns,
                    "rows": rows,
                    "message": f"Query executed successfully. Returned {len(rows)} row(s).",
                }

            conn.commit()
            affected = max(cur.rowcount, 0)
            return {
                "operation": operation,
                "columns": [],
                "rows": [],
                "message": f"{operation.upper()} executed successfully. Rows affected: {affected}.",
            }

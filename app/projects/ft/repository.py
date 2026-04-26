import hashlib
import json
import math
from datetime import date
from typing import Any

import oracledb

from app.core.db import get_db_connection


FT_DISABLE_LLM_TOGGLE = "FT_DISABLE_LLM_PROCESSING"
FT_ACCOUNTS_KV_KEY = "ft_accounts"
GOOGLE_API_KEY_KV = "GOOGLE_LLM_API_KEY"
FT_MODEL_KV = "FT_LLM_MODEL"
FT_FAST_MODEL_KV = "FT_LLM_MODEL_FAST"
FT_BATCH_LIMIT_KV = "FT_LLM_BATCH_LIMIT"
FT_PERSIST_DELAY_MS_KV = "FT_LLM_PERSIST_DELAY_MS"


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        return value.read() or ""
    return str(value)


def _normalize_account_type(account_type: str) -> str:
    t = (account_type or "").strip().upper()
    aliases = {
        "SAV": "SAVINGS",
        "SAVINGS": "SAVINGS",
        "S": "SAVINGS",
        "CR": "CREDIT",
        "CREDIT": "CREDIT",
        "C": "CREDIT",
    }
    if t not in aliases:
        raise ValueError("Account type must be savings or credit")
    return aliases[t]


def _normalize_direction(direction: str) -> str:
    d = (direction or "").strip().upper()
    if d not in {"INCOME", "EXPENSE"}:
        raise ValueError("Direction must be INCOME or EXPENSE")
    return d


def _normalize_amount(amount: float | int | str, direction: str) -> float:
    try:
        val = float(amount)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid amount") from exc
    d = _normalize_direction(direction)
    abs_val = abs(val)
    return abs_val if d == "INCOME" else -abs_val


def _normalize_date(value: str | None) -> date:
    text = (value or "").strip()
    if not text:
        return date.today()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("Date must be in YYYY-MM-DD format") from exc


def is_llm_processing_disabled() -> bool:
    sql = """
        SELECT item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(NVL(category, '')) = 'toggle'
          AND is_active = 'Y'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": FT_DISABLE_LLM_TOGGLE})
            row = cur.fetchone()
            if not row:
                return False
            return (row[0] or "N").strip().upper() == "Y"


def _get_kv_value(item_key: str) -> str | None:
    sql = """
        SELECT additional_info, item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND is_active = 'Y'
        ORDER BY updated_at DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            row = cur.fetchone()
            if not row:
                return None
            return ((row[0] or "").strip() or (row[1] or "").strip()) or None


def _get_kv_pair(item_key: str) -> tuple[str | None, str | None]:
    sql = """
        SELECT additional_info, item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND is_active = 'Y'
        ORDER BY updated_at DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            row = cur.fetchone()
            if not row:
                return None, None
            return ((row[0] or "").strip() or None, (row[1] or "").strip() or None)


def _extract_google_api_key(raw: str | None) -> str | None:
    if not raw:
        return None

    cleaned = str(raw).strip().strip('"').strip("'")
    if cleaned.lower().startswith("bearer "):
        cleaned = cleaned[7:].strip()

    # Direct key form (AI Studio keys typically start with AIza)
    if cleaned.startswith("AIza") and len(cleaned) >= 30:
        return cleaned

    # JSON form support: {"api_key":"..."}
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return None

    if isinstance(data, dict):
        for k in ("api_key", "key", "google_api_key", "GOOGLE_LLM_API_KEY"):
            val = data.get(k)
            if isinstance(val, str):
                v = val.strip().strip('"').strip("'")
                if v.startswith("AIza") and len(v) >= 30:
                    return v
    return None


def get_google_llm_api_key() -> str:
    additional_info, item_value = _get_kv_pair(GOOGLE_API_KEY_KV)
    # Try both columns and JSON forms; return first valid-looking Google key.
    key = _extract_google_api_key(item_value) or _extract_google_api_key(additional_info)
    if not key:
        raise ValueError("GOOGLE_LLM_API_KEY is missing")
    return key


def get_ft_model_name() -> str:
    additional_info, item_value = _get_kv_pair(FT_MODEL_KV)
    # For model config, prefer item_value (e.g. gemma-3-27b-it).
    return item_value or additional_info or "gemma-3-27b-it"


def _get_int_kv(item_key: str, default: int, min_value: int, max_value: int) -> int:
    raw = _get_kv_value(item_key)
    if raw is None:
        return default
    try:
        val = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, val))


def get_ft_fast_model_name() -> str | None:
    additional_info, item_value = _get_kv_pair(FT_FAST_MODEL_KV)
    model = (item_value or additional_info or "").strip()
    return model or None


def get_ft_batch_limit(default: int = 20) -> int:
    return _get_int_kv(FT_BATCH_LIMIT_KV, default=default, min_value=1, max_value=100)


def get_ft_persist_delay_ms(default: int = 0) -> int:
    return _get_int_kv(FT_PERSIST_DELAY_MS_KV, default=default, min_value=0, max_value=2000)


def list_accounts(active_only: bool = False) -> list[dict]:
    where = "WHERE is_active = 'Y'" if active_only else ""
    sql = f"""
        SELECT account_id, account_name, account_type, is_active, created_at, updated_at
        FROM ft_accounts
        {where}
        ORDER BY account_name
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [
                {
                    "account_id": int(r[0]),
                    "account_name": r[1],
                    "account_type": r[2],
                    "is_active": r[3],
                    "created_at": r[4],
                    "updated_at": r[5],
                }
                for r in cur.fetchall()
            ]


def get_account(account_id: int) -> dict | None:
    sql = """
        SELECT account_id, account_name, account_type, is_active, created_at, updated_at
        FROM ft_accounts
        WHERE account_id = :account_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"account_id": account_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "account_id": int(row[0]),
                "account_name": row[1],
                "account_type": row[2],
                "is_active": row[3],
                "created_at": row[4],
                "updated_at": row[5],
            }


def _sync_accounts_kv(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_name, account_type
            FROM ft_accounts
            WHERE is_active = 'Y'
            ORDER BY account_name
            """
        )
        accounts = [{"type": (r[1] or "").lower()[:3], "name": r[0]} for r in cur.fetchall()]
        payload = json.dumps({"account": accounts}, separators=(",", ":"))
        cur.execute(
            """
            MERGE INTO kv_store t
            USING (
              SELECT :item_key AS item_key,
                     :item_value AS item_value,
                     :additional_info AS additional_info,
                     :category AS category
              FROM dual
            ) s
            ON (t.item_key = s.item_key)
            WHEN MATCHED THEN
              UPDATE SET t.item_value = s.item_value,
                         t.additional_info = s.additional_info,
                         t.category = s.category,
                         t.is_active = 'Y'
            WHEN NOT MATCHED THEN
              INSERT (item_key, item_value, additional_info, category, is_active)
              VALUES (s.item_key, s.item_value, s.additional_info, s.category, 'Y')
            """,
            {
                "item_key": FT_ACCOUNTS_KV_KEY,
                "item_value": "FT Accounts",
                "additional_info": payload,
                "category": "config",
            },
        )


def create_account(account_name: str, account_type: str) -> int:
    name = (account_name or "").strip()
    if not name:
        raise ValueError("Account name is required")
    if len(name) > 120:
        raise ValueError("Account name must be at most 120 characters")
    account_type = _normalize_account_type(account_type)

    sql = """
        INSERT INTO ft_accounts (account_name, account_type, is_active)
        VALUES (:account_name, :account_type, 'Y')
        RETURNING account_id INTO :account_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            try:
                cur.execute(sql, {"account_name": name, "account_type": account_type, "account_id": out_id})
                _sync_accounts_kv(conn)
                conn.commit()
                return int(out_id.getvalue()[0])
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Account name already exists") from exc
                raise


def update_account(account_id: int, account_name: str, account_type: str, is_active: str = "Y") -> bool:
    name = (account_name or "").strip()
    if not name:
        raise ValueError("Account name is required")
    if len(name) > 120:
        raise ValueError("Account name must be at most 120 characters")
    account_type = _normalize_account_type(account_type)
    active = (is_active or "Y").strip().upper()
    if active not in {"Y", "N"}:
        raise ValueError("Status must be Y or N")

    sql = """
        UPDATE ft_accounts
        SET account_name = :account_name,
            account_type = :account_type,
            is_active = :is_active
        WHERE account_id = :account_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    sql,
                    {
                        "account_id": account_id,
                        "account_name": name,
                        "account_type": account_type,
                        "is_active": active,
                    },
                )
                _sync_accounts_kv(conn)
                conn.commit()
                return cur.rowcount > 0
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Account name already exists") from exc
                raise


def delete_account(account_id: int) -> bool:
    sql = "DELETE FROM ft_accounts WHERE account_id = :account_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"account_id": account_id})
            _sync_accounts_kv(conn)
            conn.commit()
            return cur.rowcount > 0


def list_transactions(
    search: str | None,
    status: str | None,
    direction: str | None,
    start_date: str | None,
    end_date: str | None,
    account_id: int | None,
    page: int,
    page_size: int,
    exclude_pending: bool = False,
) -> tuple[list[dict], int, str, str]:
    search = (search or "").strip().lower()
    status = (status or "all").strip().upper()
    direction = (direction or "all").strip().upper()
    if status not in {"ALL", "PENDING", "PROCESSED", "FAILED", "MANUAL"}:
        status = "ALL"
    if direction not in {"ALL", "INCOME", "EXPENSE"}:
        direction = "ALL"

    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}
    if search:
        where.append(
            "(LOWER(raw_text) LIKE :search OR LOWER(NVL(description, '')) LIKE :search OR LOWER(NVL(category, '')) LIKE :search)"
        )
        params["search"] = f"%{search}%"
    if status != "ALL":
        where.append("status = :status")
        params["status"] = status
    if direction != "ALL":
        where.append("direction = :direction")
        params["direction"] = direction
    if account_id is not None:
        where.append("account_id = :account_id")
        params["account_id"] = int(account_id)

    if exclude_pending:
        if status == "PENDING":
            status = "ALL"
        where.append("status <> 'PENDING'")

    if (start_date or "").strip():
        where.append("tx_date >= :start_date")
        params["start_date"] = _normalize_date(start_date)
    if (end_date or "").strip():
        where.append("tx_date <= :end_date")
        params["end_date"] = _normalize_date(end_date)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    count_sql = f"SELECT COUNT(*) FROM ft_transactions {where_sql}"
    list_sql = f"""
        SELECT t.transaction_id, t.raw_text, t.amount, t.tx_date, t.category, t.description,
               t.status, t.direction, t.created_at, t.updated_at, a.account_name, a.account_type, t.account_id
        FROM ft_transactions t
        LEFT JOIN ft_accounts a ON a.account_id = t.account_id
        {where_sql}
        ORDER BY t.tx_date DESC, t.updated_at DESC, t.transaction_id DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = int(cur.fetchone()[0])

            q_params = dict(params)
            q_params.update({"offset": offset, "limit": page_size})
            cur.execute(list_sql, q_params)
            rows = [
                {
                    "transaction_id": int(r[0]),
                    "raw_text": _to_text(r[1]),
                    "amount": float(r[2]) if r[2] is not None else 0.0,
                    "tx_date": r[3],
                    "category": r[4],
                    "description": r[5],
                    "status": r[6],
                    "direction": r[7],
                    "created_at": r[8],
                    "updated_at": r[9],
                    "account_name": r[10],
                    "account_type": r[11],
                    "account_id": int(r[12]) if r[12] is not None else None,
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, status, direction


def get_transaction(transaction_id: int) -> dict | None:
    sql = """
        SELECT t.transaction_id, t.raw_text, t.amount, t.tx_date, t.category, t.description,
               t.status, t.direction, t.llm_processed_at, t.created_at, t.updated_at,
               t.account_id, a.account_name, a.account_type
        FROM ft_transactions t
        LEFT JOIN ft_accounts a ON a.account_id = t.account_id
        WHERE t.transaction_id = :transaction_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"transaction_id": transaction_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "transaction_id": int(row[0]),
                "raw_text": _to_text(row[1]),
                "amount": float(row[2]) if row[2] is not None else 0.0,
                "tx_date": row[3],
                "category": row[4],
                "description": row[5],
                "status": row[6],
                "direction": row[7],
                "llm_processed_at": row[8],
                "created_at": row[9],
                "updated_at": row[10],
                "account_id": int(row[11]) if row[11] is not None else None,
                "account_name": row[12],
                "account_type": row[13],
            }


def create_transaction(
    raw_text: str,
    amount: float | int | str,
    direction: str,
    tx_date: str | None,
    category: str | None,
    description: str | None,
    account_id: int | None,
    status: str = "PENDING",
) -> int:
    d = _normalize_direction(direction)
    amt = _normalize_amount(amount, d)
    txd = _normalize_date(tx_date)
    cat = (category or "").strip() or "uncategorized"
    desc = (description or "").strip() or None
    raw = (raw_text or "").strip() or desc or cat
    st = (status or "PENDING").strip().upper()
    if st not in {"PENDING", "PROCESSED", "FAILED", "MANUAL"}:
        raise ValueError("Invalid status")

    sql = """
        INSERT INTO ft_transactions (raw_text, amount, tx_date, category, description, account_id, status, direction)
        VALUES (:raw_text, :amount, :tx_date, :category, :description, :account_id, :status, :direction)
        RETURNING transaction_id INTO :transaction_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            cur.execute(
                sql,
                {
                    "raw_text": raw,
                    "amount": amt,
                    "tx_date": txd,
                    "category": cat,
                    "description": desc,
                    "account_id": account_id,
                    "status": st,
                    "direction": d,
                    "transaction_id": out_id,
                },
            )
            conn.commit()
            return int(out_id.getvalue()[0])


def update_transaction(
    transaction_id: int,
    raw_text: str,
    amount: float | int | str,
    direction: str,
    tx_date: str | None,
    category: str | None,
    description: str | None,
    account_id: int | None,
    status: str,
) -> bool:
    d = _normalize_direction(direction)
    amt = _normalize_amount(amount, d)
    txd = _normalize_date(tx_date)
    cat = (category or "").strip() or "uncategorized"
    desc = (description or "").strip() or None
    raw = (raw_text or "").strip() or desc or cat
    st = (status or "PENDING").strip().upper()
    if st not in {"PENDING", "PROCESSED", "FAILED", "MANUAL"}:
        raise ValueError("Invalid status")

    sql = """
        UPDATE ft_transactions
        SET raw_text = :raw_text,
            amount = :amount,
            tx_date = :tx_date,
            category = :category,
            description = :description,
            account_id = :account_id,
            status = :status,
            direction = :direction
        WHERE transaction_id = :transaction_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "transaction_id": transaction_id,
                    "raw_text": raw,
                    "amount": amt,
                    "tx_date": txd,
                    "category": cat,
                    "description": desc,
                    "account_id": account_id,
                    "status": st,
                    "direction": d,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def update_transaction_from_llm(
    transaction_id: int,
    amount: float,
    direction: str,
    tx_date: str,
    category: str,
    description: str,
    account_id: int | None,
) -> bool:
    d = _normalize_direction(direction)
    amt = _normalize_amount(amount, d)
    txd = _normalize_date(tx_date)
    cat = (category or "").strip() or "uncategorized"
    desc = (description or "").strip() or None

    sql = """
        UPDATE ft_transactions
        SET amount = :amount,
            direction = :direction,
            tx_date = :tx_date,
            category = :category,
            description = :description,
            account_id = :account_id,
            status = 'PROCESSED',
            llm_processed_at = SYSTIMESTAMP
        WHERE transaction_id = :transaction_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "transaction_id": transaction_id,
                    "amount": amt,
                    "direction": d,
                    "tx_date": txd,
                    "category": cat,
                    "description": desc,
                    "account_id": account_id,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def mark_transaction_failed(transaction_id: int) -> None:
    sql = "UPDATE ft_transactions SET status = 'FAILED' WHERE transaction_id = :transaction_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"transaction_id": transaction_id})
            conn.commit()


def mark_transaction_pending(transaction_id: int) -> bool:
    sql = """
        UPDATE ft_transactions
        SET status = 'PENDING',
            llm_processed_at = NULL
        WHERE transaction_id = :transaction_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"transaction_id": transaction_id})
            conn.commit()
            return cur.rowcount > 0


def delete_transaction(transaction_id: int) -> bool:
    sql = "DELETE FROM ft_transactions WHERE transaction_id = :transaction_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"transaction_id": transaction_id})
            conn.commit()
            return cur.rowcount > 0


def list_pending_transactions(limit: int = 50) -> list[dict]:
    cap = max(1, min(int(limit), 500))
    sql = """
        SELECT transaction_id, raw_text, amount, tx_date, category, description, account_id, direction, status
        FROM ft_transactions
        WHERE status = 'PENDING'
        ORDER BY created_at
        FETCH FIRST :limit ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": cap})
            return [
                {
                    "transaction_id": int(r[0]),
                    "raw_text": _to_text(r[1]),
                    "amount": float(r[2]) if r[2] is not None else 0.0,
                    "tx_date": r[3].isoformat() if hasattr(r[3], "isoformat") else str(r[3]),
                    "category": r[4],
                    "description": r[5],
                    "account_id": int(r[6]) if r[6] is not None else None,
                    "direction": r[7],
                    "status": r[8],
                }
                for r in cur.fetchall()
            ]


def get_finance_summary() -> dict:
    sql = """
        SELECT
            NVL(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS total_income,
            NVL(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0) AS total_expense,
            NVL(SUM(amount), 0) AS net_amount,
            SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
        FROM ft_transactions
    """
    recent_sql = """
        SELECT t.transaction_id, t.tx_date, t.amount, t.category, t.description, t.status, t.direction,
               a.account_name
        FROM ft_transactions t
        LEFT JOIN ft_accounts a ON a.account_id = t.account_id
        ORDER BY t.tx_date DESC, t.updated_at DESC, t.transaction_id DESC
        FETCH FIRST 10 ROWS ONLY
    """
    llm_counts_sql = """
        SELECT
            COUNT(*) AS total_calls,
            SUM(CASE WHEN TRUNC(created_at) = TRUNC(SYSTIMESTAMP) THEN 1 ELSE 0 END) AS today_calls
        FROM ft_llm_calls
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone() or (0, 0, 0, 0)
            cur.execute(recent_sql)
            recent = [
                {
                    "transaction_id": int(r[0]),
                    "tx_date": r[1],
                    "amount": float(r[2] or 0),
                    "category": r[3],
                    "description": r[4],
                    "status": r[5],
                    "direction": r[6],
                    "account_name": r[7],
                }
                for r in cur.fetchall()
            ]
            cur.execute(llm_counts_sql)
            llm_row = cur.fetchone() or (0, 0)
    return {
        "total_income": float(row[0] or 0),
        "total_expense": float(row[1] or 0),
        "net_amount": float(row[2] or 0),
        "pending_count": int(row[3] or 0),
        "llm_calls_total": int(llm_row[0] or 0),
        "llm_calls_today": int(llm_row[1] or 0),
        "recent": recent,
    }


def resolve_account_id_by_name(name: str | None) -> int | None:
    if not name:
        return None
    sql = """
        SELECT account_id
        FROM ft_accounts
        WHERE LOWER(account_name) = :name
          AND is_active = 'Y'
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"name": (name or "").strip().lower()})
            row = cur.fetchone()
            return int(row[0]) if row else None


def account_context_payload() -> list[dict]:
    accounts = list_accounts(active_only=True)
    if accounts:
        return [{"name": a["account_name"], "type": a["account_type"]} for a in accounts]
    additional_info, item_value = _get_kv_pair(FT_ACCOUNTS_KV_KEY)
    # For account context JSON, prefer additional_info payload.
    raw = additional_info or item_value
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    rows = data.get("account") if isinstance(data, dict) else []
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({"name": str(row.get("name") or "").strip(), "type": str(row.get("type") or "").strip()})
    return [r for r in out if r["name"]]


def llm_cache_key(raw_text: str, accounts: list[dict], model_name: str) -> str:
    payload = json.dumps({"raw_text": raw_text, "accounts": accounts, "model": model_name}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def find_cached_llm_result(request_hash: str, model_name: str) -> dict | None:
    sql = """
        SELECT normalized_result_json
        FROM ft_llm_calls
        WHERE request_hash = :request_hash
          AND model_name = :model_name
          AND error_message IS NULL
          AND NVL(cache_hit, 'N') = 'N'
          AND normalized_result_json IS NOT NULL
        ORDER BY created_at DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"request_hash": request_hash, "model_name": model_name})
            row = cur.fetchone()
            if not row:
                return None
            text = _to_text(row[0])
            if not text:
                return None
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return None


def log_llm_call(
    transaction_id: int | None,
    model_name: str,
    request_payload: str,
    response_payload: str,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    total_tokens: int | None,
    latency_ms: int | None,
    http_status: int | None,
    cache_hit: bool,
    error_message: str | None,
    request_hash: str,
    normalized_result_json: str | None,
) -> None:
    sql = """
        INSERT INTO ft_llm_calls (
            transaction_id, model_name, request_payload, response_payload,
            prompt_tokens, completion_tokens, total_tokens,
            latency_ms, http_status, cache_hit, error_message,
            request_hash, normalized_result_json
        )
        VALUES (
            :transaction_id, :model_name, :request_payload, :response_payload,
            :prompt_tokens, :completion_tokens, :total_tokens,
            :latency_ms, :http_status, :cache_hit, :error_message,
            :request_hash, :normalized_result_json
        )
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "transaction_id": transaction_id,
                    "model_name": model_name,
                    "request_payload": request_payload,
                    "response_payload": response_payload,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "latency_ms": latency_ms,
                    "http_status": http_status,
                    "cache_hit": "Y" if cache_hit else "N",
                    "error_message": error_message,
                    "request_hash": request_hash,
                    "normalized_result_json": normalized_result_json,
                },
            )
            conn.commit()


def list_llm_calls(page: int, page_size: int, transaction_id: int | None = None) -> tuple[list[dict], int]:
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}
    if transaction_id is not None:
        where.append("c.transaction_id = :transaction_id")
        params["transaction_id"] = int(transaction_id)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    count_sql = f"SELECT COUNT(*) FROM ft_llm_calls c {where_sql}"
    list_sql = f"""
        SELECT c.call_id, c.transaction_id, c.model_name,
               c.prompt_tokens, c.completion_tokens, c.total_tokens,
               c.latency_ms, c.http_status, c.cache_hit, c.error_message,
               c.created_at
        FROM ft_llm_calls c
        {where_sql}
        ORDER BY c.created_at DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = int(cur.fetchone()[0])

            q_params = dict(params)
            q_params.update({"offset": offset, "limit": page_size})
            cur.execute(list_sql, q_params)
            rows = [
                {
                    "call_id": int(r[0]),
                    "transaction_id": int(r[1]) if r[1] is not None else None,
                    "model_name": r[2],
                    "prompt_tokens": int(r[3] or 0),
                    "completion_tokens": int(r[4] or 0),
                    "total_tokens": int(r[5] or 0),
                    "latency_ms": int(r[6] or 0),
                    "http_status": int(r[7] or 0) if r[7] is not None else None,
                    "cache_hit": r[8],
                    "error_message": r[9],
                    "created_at": r[10],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages


def get_llm_call(call_id: int) -> dict | None:
    sql = """
        SELECT call_id, transaction_id, model_name,
               request_payload, response_payload,
               prompt_tokens, completion_tokens, total_tokens,
               latency_ms, http_status, cache_hit, error_message,
               request_hash, normalized_result_json, created_at
        FROM ft_llm_calls
        WHERE call_id = :call_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"call_id": call_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "call_id": int(row[0]),
                "transaction_id": int(row[1]) if row[1] is not None else None,
                "model_name": row[2],
                "request_payload": _to_text(row[3]),
                "response_payload": _to_text(row[4]),
                "prompt_tokens": int(row[5] or 0),
                "completion_tokens": int(row[6] or 0),
                "total_tokens": int(row[7] or 0),
                "latency_ms": int(row[8] or 0),
                "http_status": int(row[9] or 0) if row[9] is not None else None,
                "cache_hit": row[10],
                "error_message": row[11],
                "request_hash": row[12],
                "normalized_result_json": _to_text(row[13]),
                "created_at": row[14],
            }

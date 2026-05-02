import json
import math
from typing import Any

import oracledb

from app.core.api_keys import verify_api_key
from app.core.db import get_db_connection


API_KEY_CATEGORY = "api_key"
API_KEY_HEADER_CONFIG_KEY = "API_KEY_HEADER_NAME"


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        data = value.read()
        return data or ""
    return str(value)


def _normalize_status(value: str | None) -> str:
    status = (value or "active").strip().lower()
    if status not in {"active", "inactive", "all"}:
        return "active"
    return status


def _parse_metadata(raw: str | None) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _metadata_json(
    *,
    name: str,
    prefix: str,
    last4: str,
    notes: str | None,
    rotated_from: str | None,
) -> str:
    payload: dict[str, Any] = {
        "name": name,
        "prefix": prefix,
        "last4": last4,
    }
    if (notes or "").strip():
        payload["notes"] = (notes or "").strip()
    if (rotated_from or "").strip():
        payload["rotated_from"] = (rotated_from or "").strip()
    return json.dumps(payload)


def validate_api_key_input(name: str, notes: str | None) -> str:
    clean_name = (name or "").strip()
    if not clean_name:
        raise ValueError("Name is required")
    if len(clean_name) > 120:
        raise ValueError("Name must be at most 120 characters")
    if notes and len((notes or "").strip()) > 4000:
        raise ValueError("Notes must be at most 4000 characters")
    return clean_name


def list_api_keys(search: str | None, status: str | None, page: int, page_size: int) -> tuple[list[dict], int, str]:
    search = (search or "").strip().lower()
    status = _normalize_status(status)
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 20), 100))
    offset = (page - 1) * page_size

    where = ["LOWER(TRIM(NVL(category, ''))) = :category"]
    params: dict[str, object] = {"category": API_KEY_CATEGORY}

    if status == "active":
        where.append("is_active = 'Y'")
    elif status == "inactive":
        where.append("is_active = 'N'")

    if search:
        where.append("(LOWER(item_key) LIKE :search OR LOWER(NVL(additional_info, '')) LIKE :search)")
        params["search"] = f"%{search}%"

    where_sql = f"WHERE {' AND '.join(where)}"
    count_sql = f"SELECT COUNT(*) FROM kv_store {where_sql}"
    list_sql = f"""
        SELECT item_key, item_value, additional_info, is_active, created_at, updated_at
        FROM kv_store
        {where_sql}
        ORDER BY updated_at DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = int((cur.fetchone() or [0])[0] or 0)

            q_params = dict(params)
            q_params.update({"offset": offset, "limit": page_size})
            cur.execute(list_sql, q_params)
            rows = []
            for r in cur.fetchall():
                md = _parse_metadata(_coerce_text(r[2]))
                rows.append(
                    {
                        "item_key": r[0],
                        "key_hash": r[1],
                        "name": md.get("name") or r[0],
                        "prefix": md.get("prefix") or "",
                        "last4": md.get("last4") or "",
                        "notes": md.get("notes") or "",
                        "rotated_from": md.get("rotated_from"),
                        "is_active": r[3],
                        "created_at": r[4],
                        "updated_at": r[5],
                    }
                )

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, status


def get_api_key(item_key: str) -> dict | None:
    sql = """
        SELECT item_key, item_value, additional_info, category, is_active, created_at, updated_at
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": API_KEY_CATEGORY})
            row = cur.fetchone()
            if not row:
                return None
            md = _parse_metadata(_coerce_text(row[2]))
            return {
                "item_key": row[0],
                "key_hash": row[1],
                "name": md.get("name") or row[0],
                "prefix": md.get("prefix") or "",
                "last4": md.get("last4") or "",
                "notes": md.get("notes") or "",
                "rotated_from": md.get("rotated_from"),
                "category": row[3],
                "is_active": row[4],
                "created_at": row[5],
                "updated_at": row[6],
            }


def create_api_key_record(
    *,
    item_key: str,
    key_hash: str,
    name: str,
    prefix: str,
    last4: str,
    notes: str | None,
    rotated_from: str | None = None,
) -> None:
    clean_name = validate_api_key_input(name, notes)
    sql = """
        INSERT INTO kv_store (item_key, item_value, additional_info, category, is_active)
        VALUES (:item_key, :item_value, :additional_info, :category, 'Y')
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    sql,
                    {
                        "item_key": (item_key or "").strip(),
                        "item_value": (key_hash or "").strip(),
                        "additional_info": _metadata_json(
                            name=clean_name,
                            prefix=(prefix or "").strip(),
                            last4=(last4 or "").strip(),
                            notes=notes,
                            rotated_from=rotated_from,
                        ),
                        "category": API_KEY_CATEGORY,
                    },
                )
                conn.commit()
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("API key reference already exists") from exc
                raise


def activate_api_key(item_key: str) -> bool:
    sql = """
        UPDATE kv_store
        SET is_active = 'Y'
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": API_KEY_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def deactivate_api_key(item_key: str) -> bool:
    sql = """
        UPDATE kv_store
        SET is_active = 'N'
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": API_KEY_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def delete_api_key_permanent(item_key: str) -> bool:
    sql = """
        DELETE FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": API_KEY_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def rotate_api_key_record(
    *,
    old_item_key: str,
    new_item_key: str,
    new_key_hash: str,
    new_name: str,
    prefix: str,
    last4: str,
    notes: str | None,
) -> None:
    clean_name = validate_api_key_input(new_name, notes)

    insert_sql = """
        INSERT INTO kv_store (item_key, item_value, additional_info, category, is_active)
        VALUES (:item_key, :item_value, :additional_info, :category, 'Y')
    """
    deactivate_sql = """
        UPDATE kv_store
        SET is_active = 'N'
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = :category
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    insert_sql,
                    {
                        "item_key": (new_item_key or "").strip(),
                        "item_value": (new_key_hash or "").strip(),
                        "additional_info": _metadata_json(
                            name=clean_name,
                            prefix=(prefix or "").strip(),
                            last4=(last4 or "").strip(),
                            notes=notes,
                            rotated_from=old_item_key,
                        ),
                        "category": API_KEY_CATEGORY,
                    },
                )

                cur.execute(deactivate_sql, {"item_key": old_item_key, "category": API_KEY_CATEGORY})
                conn.commit()
            except oracledb.IntegrityError as exc:
                conn.rollback()
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("API key reference already exists") from exc
                raise


def list_active_api_key_hashes() -> list[dict]:
    sql = """
        SELECT item_key, item_value, additional_info
        FROM kv_store
        WHERE LOWER(TRIM(NVL(category, ''))) = :category
          AND is_active = 'Y'
        ORDER BY updated_at DESC
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"category": API_KEY_CATEGORY})
            rows = cur.fetchall()

    out = []
    for r in rows:
        md = _parse_metadata(_coerce_text(r[2]))
        out.append(
            {
                "item_key": r[0],
                "key_hash": r[1],
                "name": md.get("name") or r[0],
                "prefix": md.get("prefix") or "",
                "last4": md.get("last4") or "",
            }
        )
    return out


def find_active_api_key_match(raw_key: str) -> dict | None:
    raw = (raw_key or "").strip()
    if not raw:
        return None
    for row in list_active_api_key_hashes():
        if verify_api_key(raw, str(row.get("key_hash") or "")):
            return row
    return None


def get_api_key_header_name() -> str:
    sql = """
        SELECT item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(TRIM(NVL(category, ''))) = 'config'
          AND is_active = 'Y'
        ORDER BY updated_at DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": API_KEY_HEADER_CONFIG_KEY})
            row = cur.fetchone()
            if not row:
                return "X-API-Key"
            header_name = (row[0] or "").strip()
            return header_name or "X-API-Key"


def extract_api_key_from_request(request) -> str | None:
    header_name = get_api_key_header_name()
    raw = (request.headers.get(header_name) or "").strip()
    if raw:
        return raw

    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        return token or None
    return None

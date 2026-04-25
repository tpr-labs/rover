import math
import re

import oracledb

from app.core.db import get_db_connection


KEY_RE = re.compile(r"^[A-Za-z0-9_:\-.]{1,120}$")
TOGGLE_CATEGORY = "toggle"


def _normalize_status(status: str | None) -> str:
    value = (status or "active").strip().lower()
    if value not in {"active", "inactive", "all"}:
        return "active"
    return value


def _normalize_toggle_value(value: str | None) -> str:
    val = (value or "N").strip().upper()
    if val not in {"Y", "N"}:
        raise ValueError("Toggle value must be Y or N")
    return val


def validate_toggle_input(item_key: str, additional_info: str | None, item_value: str | None) -> None:
    if not KEY_RE.fullmatch((item_key or "").strip()):
        raise ValueError("Key must be 1-120 chars and contain only letters, numbers, _ : - .")
    _normalize_toggle_value(item_value)
    if additional_info and len(additional_info) > 4000:
        raise ValueError("Additional info must be at most 4000 characters")


def list_toggles(search: str | None, status: str | None, page: int, page_size: int) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size
    status = _normalize_status(status)

    where = ["LOWER(NVL(category, '')) = :category"]
    params: dict[str, object] = {"category": TOGGLE_CATEGORY}

    if status == "active":
        where.append("is_active = 'Y'")
    elif status == "inactive":
        where.append("is_active = 'N'")

    if search:
        where.append("(LOWER(item_key) LIKE :search OR LOWER(NVL(additional_info, '')) LIKE :search)")
        params["search"] = f"%{search.lower()}%"

    where_sql = f"WHERE {' AND '.join(where)}"

    count_sql = f"SELECT COUNT(*) FROM kv_store {where_sql}"
    list_sql = f"""
        SELECT item_key, item_value, additional_info, is_active, created_at, updated_at
        FROM kv_store
        {where_sql}
        ORDER BY item_key
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
                    "item_key": r[0],
                    "item_value": r[1],
                    "additional_info": r[2],
                    "is_active": r[3],
                    "created_at": r[4],
                    "updated_at": r[5],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, status


def get_toggle(item_key: str) -> dict | None:
    sql = """
        SELECT item_key, item_value, additional_info, category, is_active, created_at, updated_at
        FROM kv_store
        WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": TOGGLE_CATEGORY})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "item_key": row[0],
                "item_value": row[1],
                "additional_info": row[2],
                "category": row[3],
                "is_active": row[4],
                "created_at": row[5],
                "updated_at": row[6],
            }


def create_toggle(item_key: str, additional_info: str | None, item_value: str) -> None:
    validate_toggle_input(item_key, additional_info, item_value)
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
                        "item_key": item_key.strip(),
                        "item_value": _normalize_toggle_value(item_value),
                        "additional_info": (additional_info or "").strip() or None,
                        "category": TOGGLE_CATEGORY,
                    },
                )
                conn.commit()
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Key already exists") from exc
                raise


def update_toggle(item_key: str, additional_info: str | None, item_value: str, is_active: str = "Y") -> bool:
    validate_toggle_input(item_key, additional_info, item_value)
    active_val = (is_active or "Y").strip().upper()
    if active_val not in {"Y", "N"}:
        raise ValueError("Status must be Y or N")

    sql = """
        UPDATE kv_store
        SET item_value = :item_value,
            additional_info = :additional_info,
            is_active = :is_active,
            category = :category
        WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "item_key": item_key,
                    "item_value": _normalize_toggle_value(item_value),
                    "additional_info": (additional_info or "").strip() or None,
                    "is_active": active_val,
                    "category": TOGGLE_CATEGORY,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def switch_toggle(item_key: str, desired_value: str) -> bool:
    value = _normalize_toggle_value(desired_value)
    sql = """
        UPDATE kv_store
        SET item_value = :item_value
        WHERE item_key = :item_key
          AND LOWER(NVL(category, '')) = :category
          AND is_active = 'Y'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "item_value": value, "category": TOGGLE_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def deactivate_toggle(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'N' WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": TOGGLE_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def restore_toggle(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'Y' WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": TOGGLE_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def delete_toggle(item_key: str) -> bool:
    sql = "DELETE FROM kv_store WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": TOGGLE_CATEGORY})
            conn.commit()
            return cur.rowcount > 0

import math
import re

import oracledb

from app.core.db import get_db_connection


KEY_RE = re.compile(r"^[A-Za-z0-9_:\-.]{1,120}$")
SHORTCUT_CATEGORY = "shortcut"


def _normalize_status(status: str | None) -> str:
    value = (status or "active").strip().lower()
    if value not in {"active", "inactive", "all"}:
        return "active"
    return value


def validate_shortcut_input(item_key: str, item_value: str, path: str | None) -> None:
    if not KEY_RE.fullmatch((item_key or "").strip()):
        raise ValueError("Unique ref must be 1-120 chars and contain only letters, numbers, _ : - .")

    name = (item_value or "").strip()
    if not name:
        raise ValueError("Shortcut name is required")
    if len(name) > 500:
        raise ValueError("Shortcut name must be at most 500 characters")

    target = (path or "").strip()
    if not target:
        raise ValueError("Path is required")
    if not target.startswith("/"):
        raise ValueError("Path must start with /")
    if target.startswith("//"):
        raise ValueError("Path cannot start with //")
    if target.lower().startswith("http://") or target.lower().startswith("https://"):
        raise ValueError("Only app-internal paths are allowed")
    if " " in target:
        raise ValueError("Path cannot contain spaces")
    if len(target) > 4000:
        raise ValueError("Path must be at most 4000 characters")


def list_shortcuts(search: str | None, status: str | None, page: int, page_size: int) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size
    status = _normalize_status(status)

    where = ["LOWER(NVL(category, '')) = :category"]
    params: dict[str, object] = {"category": SHORTCUT_CATEGORY}

    if status == "active":
        where.append("is_active = 'Y'")
    elif status == "inactive":
        where.append("is_active = 'N'")

    if search:
        where.append(
            "(LOWER(item_key) LIKE :search OR LOWER(item_value) LIKE :search OR LOWER(NVL(additional_info, '')) LIKE :search)"
        )
        params["search"] = f"%{search.lower()}%"

    where_sql = f"WHERE {' AND '.join(where)}"

    count_sql = f"SELECT COUNT(*) FROM kv_store {where_sql}"
    list_sql = f"""
        SELECT item_key, item_value, additional_info, is_active, created_at, updated_at
        FROM kv_store
        {where_sql}
        ORDER BY item_value
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


def get_shortcut(item_key: str) -> dict | None:
    sql = """
        SELECT item_key, item_value, additional_info, category, is_active, created_at, updated_at
        FROM kv_store
        WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
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


def create_shortcut(item_key: str, item_value: str, path: str) -> None:
    validate_shortcut_input(item_key, item_value, path)
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
                        "item_value": item_value.strip(),
                        "additional_info": path.strip(),
                        "category": SHORTCUT_CATEGORY,
                    },
                )
                conn.commit()
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Unique ref already exists") from exc
                raise


def update_shortcut(item_key: str, item_value: str, path: str, is_active: str = "Y") -> bool:
    validate_shortcut_input(item_key, item_value, path)
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
                    "item_value": item_value.strip(),
                    "additional_info": path.strip(),
                    "is_active": active_val,
                    "category": SHORTCUT_CATEGORY,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def deactivate_shortcut(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'N' WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def restore_shortcut(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'Y' WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def delete_shortcut(item_key: str) -> bool:
    sql = "DELETE FROM kv_store WHERE item_key = :item_key AND LOWER(NVL(category, '')) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def list_nav_shortcuts() -> list[dict]:
    sql = """
        SELECT item_key, item_value, additional_info
        FROM kv_store
        WHERE LOWER(NVL(category, '')) = :category
          AND is_active = 'Y'
        ORDER BY item_value
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"category": SHORTCUT_CATEGORY})
            rows = cur.fetchall()

    out = []
    for item_key, item_value, additional_info in rows:
        path = (additional_info or "").strip()
        if not path.startswith("/") or path.startswith("//"):
            continue
        out.append(
            {
                "key": item_key,
                "title": item_value or item_key,
                "path": path,
            }
        )
    return out

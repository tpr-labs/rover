import math
import re

import oracledb

from app.core.db import get_db_connection


KEY_RE = re.compile(r"^[A-Za-z0-9_:\-.]{1,120}$")


def _normalized_category(category: str | None) -> str:
    value = (category or "").strip()
    return value if value else "NA"


def _normalized_sub_category(sub_category: str | None) -> str | None:
    value = (sub_category or "").strip()
    return value or None


def validate_key_value_input(
    item_key: str,
    item_value: str,
    additional_info: str | None,
    category: str | None,
    sub_category: str | None = None,
) -> None:
    if not KEY_RE.fullmatch((item_key or "").strip()):
        raise ValueError("Key must be 1-120 chars and contain only letters, numbers, _ : - .")
    if not (item_value or "").strip():
        raise ValueError("Value is required")
    if len(item_value) > 500:
        raise ValueError("Value must be at most 500 characters")
    if additional_info and len(additional_info) > 4000:
        raise ValueError("Additional info must be at most 4000 characters")
    if category and len(category) > 100:
        raise ValueError("Category must be at most 100 characters")
    if sub_category and len(sub_category) > 100:
        raise ValueError("Sub Category must be at most 100 characters")


def _normalize_status(status: str | None) -> str:
    value = (status or "active").strip().lower()
    if value not in {"active", "inactive", "all"}:
        return "active"
    return value


def list_items(
    search: str | None,
    category: str | None,
    sub_category: str | None,
    status: str | None,
    page: int,
    page_size: int,
) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    category = (category or "").strip()
    sub_category = (sub_category or "").strip()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}
    status = _normalize_status(status)

    if status == "active":
        where.append("is_active = 'Y'")
    elif status == "inactive":
        where.append("is_active = 'N'")

    if search:
        where.append("(LOWER(item_key) LIKE :search OR LOWER(item_value) LIKE :search OR LOWER(NVL(additional_info, '')) LIKE :search)")
        params["search"] = f"%{search.lower()}%"
    if category:
        where.append("LOWER(NVL(category, '')) = :category")
        params["category"] = category.lower()
    if sub_category:
        where.append("LOWER(NVL(sub_category, '')) = :sub_category")
        params["sub_category"] = sub_category.lower()

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    count_sql = f"SELECT COUNT(*) FROM kv_store {where_sql}"
    list_sql = f"""
        SELECT item_key, item_value, additional_info, category, sub_category, is_active, created_at, updated_at
        FROM kv_store
        {where_sql}
        ORDER BY updated_at DESC
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
                    "category": r[3],
                    "sub_category": r[4],
                    "is_active": r[5],
                    "created_at": r[6],
                    "updated_at": r[7],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, status


def get_item(item_key: str) -> dict | None:
    sql = """
        SELECT item_key, item_value, additional_info, category, sub_category, is_active, created_at, updated_at
        FROM kv_store
        WHERE item_key = :item_key
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "item_key": row[0],
                "item_value": row[1],
                "additional_info": row[2],
                "category": row[3],
                "sub_category": row[4],
                "is_active": row[5],
                "created_at": row[6],
                "updated_at": row[7],
            }


def create_item(
    item_key: str,
    item_value: str,
    additional_info: str | None,
    category: str | None,
    sub_category: str | None,
) -> None:
    validate_key_value_input(item_key, item_value, additional_info, category, sub_category)
    sql = """
        INSERT INTO kv_store (item_key, item_value, additional_info, category, sub_category)
        VALUES (:item_key, :item_value, :additional_info, :category, :sub_category)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    sql,
                    {
                        "item_key": item_key.strip(),
                        "item_value": item_value.strip(),
                        "additional_info": (additional_info or "").strip() or None,
                        "category": _normalized_category(category),
                        "sub_category": _normalized_sub_category(sub_category),
                    },
                )
                conn.commit()
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Key already exists") from exc
                raise


def update_item(
    item_key: str,
    item_value: str,
    additional_info: str | None,
    category: str | None,
    sub_category: str | None,
    is_active: str = "Y",
) -> bool:
    validate_key_value_input(item_key, item_value, additional_info, category, sub_category)
    is_active = (is_active or "Y").strip().upper()
    if is_active not in {"Y", "N"}:
        raise ValueError("Status must be Y or N")

    sql = """
        UPDATE kv_store
        SET item_value = :item_value,
            additional_info = :additional_info,
            category = :category,
            sub_category = :sub_category,
            is_active = :is_active
        WHERE item_key = :item_key
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "item_key": item_key,
                    "item_value": item_value.strip(),
                    "additional_info": (additional_info or "").strip() or None,
                    "category": _normalized_category(category),
                    "sub_category": _normalized_sub_category(sub_category),
                    "is_active": is_active,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def deactivate_item(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'N' WHERE item_key = :item_key"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            conn.commit()
            return cur.rowcount > 0


def restore_item(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'Y' WHERE item_key = :item_key"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            conn.commit()
            return cur.rowcount > 0


def delete_item(item_key: str) -> bool:
    sql = "DELETE FROM kv_store WHERE item_key = :item_key"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key})
            conn.commit()
            return cur.rowcount > 0


def list_dashboard_projects() -> list[dict]:
    sql = """
        SELECT item_key, item_value
        FROM kv_store
        WHERE category = 'dashboard' AND is_active = 'Y'
        ORDER BY item_value
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    projects = []
    for key, value in rows:
        slug = (key or "").strip().lower()
        if not slug:
            continue
        projects.append(
            {
                "key": key,
                "title": value or key,
                "path": f"/{slug}",
            }
        )
    return projects

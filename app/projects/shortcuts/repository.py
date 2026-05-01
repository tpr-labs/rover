import math
import re
import json
from typing import Any

import oracledb

from app.core.db import get_db_connection


KEY_RE = re.compile(r"^[A-Za-z0-9_:\-.]{1,120}$")
SHORTCUT_CATEGORY = "shortcut"
DASHBOARD_CATEGORY = "dashboard"
ICON_CLASS_RE = re.compile(r"^[A-Za-z0-9\-\s]{3,80}$")

DEFAULT_ICON_BY_PATH = {
    "/ft": "fa-solid fa-wallet",
    "/ft/tracker": "fa-solid fa-chart-line",
    "/ft_tracker": "fa-solid fa-chart-line",
}


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        return value.read() or ""
    return str(value)


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


def validate_dashboard_item_input(item_key: str, item_value: str, additional_info: str | None) -> None:
    if not KEY_RE.fullmatch((item_key or "").strip()):
        raise ValueError("Unique ref must be 1-120 chars and contain only letters, numbers, _ : - .")

    title = (item_value or "").strip()
    if not title:
        raise ValueError("Dashboard item name is required")
    if len(title) > 500:
        raise ValueError("Dashboard item name must be at most 500 characters")

    metadata = (additional_info or "").strip()
    if len(metadata) > 4000:
        raise ValueError("Additional info must be at most 4000 characters")

    if metadata:
        try:
            payload = json.loads(metadata)
            if isinstance(payload, dict) and payload.get("icon"):
                icon = str(payload.get("icon")).strip()
                if icon and not ICON_CLASS_RE.fullmatch(icon):
                    raise ValueError("Icon class contains unsupported characters")
            if isinstance(payload, dict) and payload.get("order") is not None:
                raw_order = payload.get("order")
                if isinstance(raw_order, bool):
                    raise ValueError("Order must be a positive integer")
                try:
                    order = int(raw_order)
                except (TypeError, ValueError):
                    raise ValueError("Order must be a positive integer")
                if order < 1:
                    raise ValueError("Order must be at least 1")

            if isinstance(payload, dict) and payload.get("quick_links") is not None:
                quick_links = payload.get("quick_links")
                if not isinstance(quick_links, list):
                    raise ValueError("quick_links must be a JSON array")

                base_path = f"/{(item_key or '').strip().lower()}"
                if not base_path or base_path == "/":
                    raise ValueError("Dashboard key is invalid for quick-link validation")

                for idx, row in enumerate(quick_links, start=1):
                    if not isinstance(row, dict):
                        raise ValueError(f"quick_links[{idx}] must be an object")

                    label = str(row.get("label") or "").strip()
                    path = str(row.get("path") or "").strip()
                    if not label:
                        raise ValueError(f"quick_links[{idx}] label is required")
                    if len(label) > 80:
                        raise ValueError(f"quick_links[{idx}] label must be at most 80 chars")
                    if not path:
                        raise ValueError(f"quick_links[{idx}] path is required")
                    if len(path) > 4000:
                        raise ValueError(f"quick_links[{idx}] path must be at most 4000 chars")
                    if not path.startswith("/"):
                        raise ValueError(f"quick_links[{idx}] path must start with /")
                    if path.startswith("//"):
                        raise ValueError(f"quick_links[{idx}] path cannot start with //")
                    if path.lower().startswith("http://") or path.lower().startswith("https://"):
                        raise ValueError(f"quick_links[{idx}] external URLs are not allowed")
                    if " " in path:
                        raise ValueError(f"quick_links[{idx}] path cannot contain spaces")

                    same_project_ok = (
                        path == base_path
                        or path.startswith(base_path + "/")
                        or path.startswith(base_path + "?")
                        or path.startswith(base_path + "#")
                    )
                    if not same_project_ok:
                        raise ValueError(
                            f"quick_links[{idx}] must stay inside project '{base_path}'"
                        )
        except json.JSONDecodeError:
            raise ValueError("Additional info must be valid JSON")


def list_shortcuts(search: str | None, status: str | None, page: int, page_size: int) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size
    status = _normalize_status(status)

    where = ["LOWER(TRIM(NVL(category, ''))) = :category"]
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


def list_dashboard_items(search: str | None, status: str | None, page: int, page_size: int) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size
    status = _normalize_status(status)

    where = ["LOWER(TRIM(NVL(category, ''))) = :category"]
    params: dict[str, object] = {"category": DASHBOARD_CATEGORY}

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
        WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category
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


def get_dashboard_item(item_key: str) -> dict | None:
    sql = """
        SELECT item_key, item_value, additional_info, category, is_active, created_at, updated_at
        FROM kv_store
        WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": DASHBOARD_CATEGORY})
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


def create_dashboard_item(item_key: str, item_value: str, additional_info: str | None) -> None:
    validate_dashboard_item_input(item_key, item_value, additional_info)
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
                        "additional_info": (additional_info or "").strip() or None,
                        "category": DASHBOARD_CATEGORY,
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
        WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category
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


def update_dashboard_item(item_key: str, item_value: str, additional_info: str | None, is_active: str = "Y") -> bool:
    validate_dashboard_item_input(item_key, item_value, additional_info)
    active_val = (is_active or "Y").strip().upper()
    if active_val not in {"Y", "N"}:
        raise ValueError("Status must be Y or N")

    sql = """
        UPDATE kv_store
        SET item_value = :item_value,
            additional_info = :additional_info,
            is_active = :is_active,
            category = :category
        WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "item_key": item_key,
                    "item_value": item_value.strip(),
                    "additional_info": (additional_info or "").strip() or None,
                    "is_active": active_val,
                    "category": DASHBOARD_CATEGORY,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def deactivate_shortcut(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'N' WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def deactivate_dashboard_item(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'N' WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": DASHBOARD_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def restore_shortcut(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'Y' WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def restore_dashboard_item(item_key: str) -> bool:
    sql = "UPDATE kv_store SET is_active = 'Y' WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": DASHBOARD_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def delete_shortcut(item_key: str) -> bool:
    sql = "DELETE FROM kv_store WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": SHORTCUT_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def delete_dashboard_item(item_key: str) -> bool:
    sql = "DELETE FROM kv_store WHERE item_key = :item_key AND LOWER(TRIM(NVL(category, ''))) = :category"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": item_key, "category": DASHBOARD_CATEGORY})
            conn.commit()
            return cur.rowcount > 0


def list_nav_shortcuts() -> list[dict]:
    sql = """
        SELECT item_key, item_value, additional_info
        FROM kv_store
        WHERE LOWER(TRIM(NVL(category, ''))) IN ('shortcut', 'shortcuts')
          AND UPPER(NVL(is_active, 'N')) = 'Y'
        ORDER BY item_value
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    def _normalize_path(raw: str | None) -> str:
        value = _to_text(raw).strip()
        if not value:
            return ""
        low = value.lower()
        if low.startswith("http://") or low.startswith("https://"):
            return ""

        # If text contains an embedded internal path, extract it.
        embedded = re.search(r"/[A-Za-z0-9_\-./?=&%]*", value)
        if embedded:
            value = embedded.group(0).strip()

        # Accept slug-like values and coerce to /slug.
        if not value.startswith("/"):
            value = f"/{value}"

        # Normalize repeated slashes at start, keep one.
        value = "/" + value.lstrip("/")

        if " " in value:
            value = value.replace(" ", "-")
        return value

    def _extract_payload(raw: str | None) -> dict:
        text = _to_text(raw).strip()
        if not text.startswith("{"):
            return {}
        try:
            payload = json.loads(text)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    out: list[dict] = []
    seen_paths: set[str] = set()

    for item_key, item_value, additional_info in rows:
        key_text = _to_text(item_key).strip()
        value_text = _to_text(item_value).strip()
        info_text = _to_text(additional_info).strip()

        payload = _extract_payload(additional_info)

        title = (
            _to_text(payload.get("title") or payload.get("name") or "").strip()
            or value_text
            or key_text
            or "Shortcut"
        )

        icon_candidate = _to_text(payload.get("icon") or "").strip()
        icon_class = icon_candidate if icon_candidate and ICON_CLASS_RE.fullmatch(icon_candidate) else None

        path_candidates = [
            payload.get("path"),
            payload.get("url"),
            payload.get("route"),
            payload.get("href"),
            info_text,
            value_text,
            key_text,
        ]

        path = ""
        for candidate in path_candidates:
            path = _normalize_path(candidate)
            if path:
                break

        if not path:
            continue
        if path in seen_paths:
            continue
        seen_paths.add(path)

        if not icon_class:
            icon_class = DEFAULT_ICON_BY_PATH.get(path)

        out.append({"key": key_text or path, "title": title, "path": path, "icon_class": icon_class})

    return out

import math
import os
import re
import uuid
from urllib.parse import quote

import requests

from app.core.db import get_db_connection


UPLOAD_TOGGLE_KEY = "ALLOW_OCI_FILE_UPLOAD"
UPLOAD_PAR_KEY = "oci_file_upload_par"
READ_PAR_KEY = "oci_file_read_par"
UPLOAD_OBJECT_PREFIX = "rover-uploads"

_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename(name: str) -> str:
    base = (name or "file").strip()
    base = os.path.basename(base)
    cleaned = _SAFE_FILENAME_RE.sub("_", base).strip("._")
    return cleaned or "file"


def _normalize_par_url(raw: str) -> str:
    value = (raw or "").strip()

    # Accept common wrapped/config formats from KV storage.
    if value.startswith("{") and value.endswith("}"):
        import json

        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            for key in ("par_url", "url", "value", "oci_par", "upload_par", "read_par"):
                candidate = payload.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    value = candidate.strip()
                    break

    # Remove optional key-prefix wrappers like "PAR_URL=..."
    if "=" in value and not value.lower().startswith(("http://", "https://")):
        k, v = value.split("=", 1)
        if k.strip().lower() in {"par_url", "url", "value", "oci_par", "upload_par", "read_par"}:
            value = v.strip()

    # Remove surrounding quotes often introduced by SQL editors/copy-paste.
    value = value.strip().strip('"').strip("'")

    # Remove accidental leading Unicode BOM if present.
    if value.startswith("\ufeff"):
        value = value.lstrip("\ufeff")

    if not value:
        raise ValueError("Upload PAR URL is not configured")

    # If value contains surrounding explanatory text, extract first URL-like token.
    if not value.lower().startswith(("http://", "https://")):
        m = re.search(r"https?://\S+", value, flags=re.IGNORECASE)
        if m:
            value = m.group(0).strip().rstrip(")],;'")

    if not (value.startswith("http://") or value.startswith("https://")):
        raise ValueError("Upload PAR URL must start with http:// or https://")
    return value


def _object_url(par_url: str, object_name: str) -> str:
    base = _normalize_par_url(par_url)
    object_name = quote(object_name, safe="/._-")
    if base.endswith("/"):
        return f"{base}{object_name}"
    return f"{base}/{object_name}"


def is_upload_allowed() -> bool:
    sql = """
        SELECT item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(NVL(category, '')) = 'toggle'
          AND is_active = 'Y'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": UPLOAD_TOGGLE_KEY})
            row = cur.fetchone()
            if not row:
                return False
            return (row[0] or "N").strip().upper() == "Y"


def _get_par_url(item_key: str, key_label: str) -> str:
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
                raise ValueError(f"KV key '{key_label}' is missing")

            # Backward-compatible lookup across both columns.
            additional_info = (row[0] or "").strip()
            item_value = (row[1] or "").strip()
            if not additional_info and not item_value:
                raise ValueError(f"KV key '{key_label}' is empty (set PAR in additional_info or item_value)")

            # Try additional_info first, then fallback to item_value if first value is non-URL metadata.
            for par_candidate in (additional_info, item_value):
                if not par_candidate:
                    continue
                try:
                    return _normalize_par_url(par_candidate)
                except ValueError:
                    continue

            raise ValueError(
                f"KV key '{key_label}' does not contain a valid URL in additional_info or item_value"
            )


def get_upload_par_url() -> str:
    return _get_par_url(UPLOAD_PAR_KEY, UPLOAD_PAR_KEY)


def get_read_par_url() -> str:
    return _get_par_url(READ_PAR_KEY, READ_PAR_KEY)


def get_read_object_url(object_name: str) -> str:
    name = (object_name or "").strip()
    if not name:
        raise ValueError("Object name is missing")
    return _object_url(get_read_par_url(), name)


def list_uploads(search: str | None, page: int, page_size: int) -> tuple[list[dict], int]:
    search = (search or "").strip().lower()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}
    if search:
        where.append(
            "(LOWER(title) LIKE :search OR LOWER(original_file_name) LIKE :search OR LOWER(NVL(notes, '')) LIKE :search)"
        )
        params["search"] = f"%{search}%"

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    count_sql = f"SELECT COUNT(*) FROM uploads_files {where_sql}"
    list_sql = f"""
        SELECT upload_id, title, original_file_name, content_type, size_bytes, object_name, object_url, created_at, updated_at
        FROM uploads_files
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
                    "upload_id": int(r[0]),
                    "title": r[1],
                    "original_file_name": r[2],
                    "content_type": r[3],
                    "size_bytes": int(r[4] or 0),
                    "object_name": r[5],
                    "object_url": r[6],
                    "created_at": r[7],
                    "updated_at": r[8],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages


def get_upload(upload_id: int) -> dict | None:
    sql = """
        SELECT upload_id, title, original_file_name, content_type, size_bytes, object_name, object_url, notes, created_at, updated_at
        FROM uploads_files
        WHERE upload_id = :upload_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "upload_id": int(row[0]),
                "title": row[1],
                "original_file_name": row[2],
                "content_type": row[3],
                "size_bytes": int(row[4] or 0),
                "object_name": row[5],
                "object_url": row[6],
                "notes": row[7],
                "created_at": row[8],
                "updated_at": row[9],
            }


def validate_upload_metadata(title: str, notes: str | None) -> None:
    t = (title or "").strip()
    if not t:
        raise ValueError("Title is required")
    if len(t) > 500:
        raise ValueError("Title must be at most 500 characters")
    if notes and len(notes) > 4000:
        raise ValueError("Notes must be at most 4000 characters")


def upload_file_to_oci(original_file_name: str, file_bytes: bytes, content_type: str | None) -> tuple[str, str, int, str]:
    if not is_upload_allowed():
        raise ValueError("OCI upload is disabled by toggle ALLOW_OCI_FILE_UPLOAD")
    if not file_bytes:
        raise ValueError("File is empty")

    par_url = get_upload_par_url()
    safe_name = _safe_filename(original_file_name)
    object_name = f"{UPLOAD_OBJECT_PREFIX}/{uuid.uuid4().hex}_{safe_name}"
    put_url = _object_url(par_url, object_name)

    headers = {"Content-Type": (content_type or "application/octet-stream").strip() or "application/octet-stream"}
    response = requests.put(put_url, data=file_bytes, headers=headers, timeout=(10, 120))
    if response.status_code not in {200, 201}:
        raise ValueError(f"OCI upload failed (status {response.status_code})")

    return object_name, put_url, len(file_bytes), headers["Content-Type"]


def fetch_object_bytes(object_name: str) -> tuple[bytes, str | None]:
    url = get_read_object_url(object_name)

    response = requests.get(url, timeout=(10, 120))
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch object (status {response.status_code})")

    return response.content or b"", response.headers.get("Content-Type")


def delete_object_from_oci(object_name: str) -> None:
    if not is_upload_allowed():
        raise ValueError("OCI upload is disabled by toggle ALLOW_OCI_FILE_UPLOAD")
    par_url = get_upload_par_url()
    del_url = _object_url(par_url, object_name)
    response = requests.delete(del_url, timeout=(10, 120))
    if response.status_code not in {200, 202, 204, 404}:
        raise ValueError(f"OCI delete failed (status {response.status_code})")


def create_upload_record(
    title: str,
    original_file_name: str,
    content_type: str,
    size_bytes: int,
    object_name: str,
    object_url: str,
    notes: str | None,
) -> int:
    validate_upload_metadata(title, notes)
    sql = """
        INSERT INTO uploads_files (title, original_file_name, content_type, size_bytes, object_name, object_url, notes)
        VALUES (:title, :original_file_name, :content_type, :size_bytes, :object_name, :object_url, :notes)
        RETURNING upload_id INTO :upload_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            cur.execute(
                sql,
                {
                    "title": (title or "").strip(),
                    "original_file_name": (original_file_name or "").strip(),
                    "content_type": (content_type or "application/octet-stream").strip(),
                    "size_bytes": int(size_bytes),
                    "object_name": object_name,
                    "object_url": object_url,
                    "notes": (notes or "").strip() or None,
                    "upload_id": out_id,
                },
            )
            conn.commit()
            return int(out_id.getvalue()[0])


def update_upload_record(upload_id: int, title: str, notes: str | None) -> bool:
    validate_upload_metadata(title, notes)
    sql = """
        UPDATE uploads_files
        SET title = :title,
            notes = :notes
        WHERE upload_id = :upload_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "upload_id": upload_id,
                    "title": title.strip(),
                    "notes": (notes or "").strip() or None,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def delete_upload_record(upload_id: int) -> bool:
    sql = "DELETE FROM uploads_files WHERE upload_id = :upload_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id})
            conn.commit()
            return cur.rowcount > 0


def list_upload_links(upload_id: int) -> list[dict]:
    sql = """
        SELECT f.file_id, f.title
        FROM uploads_sb_file_links l
        JOIN sb_files f ON f.file_id = l.file_id
        WHERE l.upload_id = :upload_id
          AND f.is_trashed = 'N'
        ORDER BY f.title
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id})
            return [{"file_id": int(r[0]), "title": r[1]} for r in cur.fetchall()]


def list_link_candidates(upload_id: int) -> list[dict]:
    sql = """
        SELECT f.file_id, f.title
        FROM sb_files f
        WHERE f.is_trashed = 'N'
          AND NOT EXISTS (
            SELECT 1
            FROM uploads_sb_file_links l
            WHERE l.upload_id = :upload_id
              AND l.file_id = f.file_id
          )
        ORDER BY f.updated_at DESC
        FETCH FIRST 200 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id})
            return [{"file_id": int(r[0]), "title": r[1]} for r in cur.fetchall()]


def add_upload_link(upload_id: int, file_id: int) -> None:
    sql = """
        INSERT INTO uploads_sb_file_links (upload_id, file_id)
        VALUES (:upload_id, :file_id)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id, "file_id": file_id})
            conn.commit()


def remove_upload_link(upload_id: int, file_id: int) -> bool:
    sql = "DELETE FROM uploads_sb_file_links WHERE upload_id = :upload_id AND file_id = :file_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"upload_id": upload_id, "file_id": file_id})
            conn.commit()
            return cur.rowcount > 0

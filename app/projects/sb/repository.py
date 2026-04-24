from typing import Any

import oracledb

from app.core.db import get_db_connection


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        return value.read() or ""
    return str(value)


def ensure_trash_folder() -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT folder_id
                FROM sb_folders
                WHERE parent_folder_id IS NULL AND folder_name = 'Trash' AND is_trashed = 'N'
                FETCH FIRST 1 ROWS ONLY
                """
            )
            row = cur.fetchone()
            if row:
                return int(row[0])

            cur.execute(
                """
                INSERT INTO sb_folders (parent_folder_id, folder_name, is_trashed)
                VALUES (NULL, 'Trash', 'N')
                """
            )
            conn.commit()
            cur.execute(
                """
                SELECT folder_id
                FROM sb_folders
                WHERE parent_folder_id IS NULL AND folder_name = 'Trash' AND is_trashed = 'N'
                ORDER BY folder_id DESC
                FETCH FIRST 1 ROWS ONLY
                """
            )
            return int(cur.fetchone()[0])


def list_folders_tree() -> list[dict[str, Any]]:
    trash_id = ensure_trash_folder()
    sql = """
        SELECT folder_id, parent_folder_id, folder_name, LEVEL AS lvl
        FROM sb_folders
        WHERE is_trashed = 'N'
        START WITH parent_folder_id IS NULL
        CONNECT BY PRIOR folder_id = parent_folder_id
        ORDER SIBLINGS BY folder_name
    """
    out = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for folder_id, parent_id, name, lvl in cur.fetchall():
                if int(folder_id) == trash_id:
                    continue
                out.append(
                    {
                        "folder_id": int(folder_id),
                        "parent_folder_id": int(parent_id) if parent_id is not None else None,
                        "folder_name": name,
                        "level": int(lvl),
                    }
                )
    return out


def list_active_folders() -> list[dict[str, Any]]:
    sql = """
        SELECT folder_id, folder_name
        FROM sb_folders
        WHERE is_trashed = 'N'
        ORDER BY folder_name
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [{"folder_id": int(r[0]), "folder_name": r[1]} for r in cur.fetchall()]


def list_files(folder_id: int) -> list[dict[str, Any]]:
    sql = """
        SELECT file_id, title, tags, updated_at
        FROM sb_files
        WHERE folder_id = :folder_id AND is_trashed = 'N'
        ORDER BY updated_at DESC
    """
    out = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"folder_id": folder_id})
            for file_id, title, tags, updated_at in cur.fetchall():
                out.append(
                    {
                        "file_id": int(file_id),
                        "title": title,
                        "tags": tags,
                        "updated_at": updated_at,
                    }
                )
    return out


def search_files(query: str) -> list[dict[str, Any]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    sql = """
        SELECT file_id, title, tags, updated_at
        FROM sb_files
        WHERE is_trashed = 'N'
          AND (LOWER(title) LIKE :q OR LOWER(NVL(tags, '')) LIKE :q)
        ORDER BY updated_at DESC
        FETCH FIRST 100 ROWS ONLY
    """
    out = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"q": f"%{q}%"})
            for file_id, title, tags, updated_at in cur.fetchall():
                out.append({"file_id": int(file_id), "title": title, "tags": tags, "updated_at": updated_at})
    return out


def create_folder(name: str, parent_folder_id: int | None) -> None:
    name = (name or "").strip()
    if not name:
        raise ValueError("Folder name is required")
    sql = """
        INSERT INTO sb_folders (parent_folder_id, folder_name, is_trashed)
        VALUES (:parent_folder_id, :folder_name, 'N')
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"parent_folder_id": parent_folder_id, "folder_name": name})
                conn.commit()
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Folder name already exists under selected parent") from exc
                raise


def get_folder(folder_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT folder_id, parent_folder_id, folder_name, is_trashed, created_at, updated_at
        FROM sb_folders
        WHERE folder_id = :folder_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"folder_id": folder_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "folder_id": int(row[0]),
                "parent_folder_id": int(row[1]) if row[1] is not None else None,
                "folder_name": row[2],
                "is_trashed": row[3],
                "created_at": row[4],
                "updated_at": row[5],
            }


def update_folder(folder_id: int, folder_name: str, parent_folder_id: int | None) -> bool:
    folder_name = (folder_name or "").strip()
    if not folder_name:
        raise ValueError("Folder name is required")
    sql = """
        UPDATE sb_folders
        SET folder_name = :folder_name,
            parent_folder_id = :parent_folder_id
        WHERE folder_id = :folder_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"folder_id": folder_id, "folder_name": folder_name, "parent_folder_id": parent_folder_id})
                conn.commit()
                return cur.rowcount > 0
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Folder name already exists under selected parent") from exc
                raise


def trash_folder(folder_id: int) -> bool:
    trash_id = ensure_trash_folder()
    if folder_id == trash_id:
        raise ValueError("Trash folder cannot be deleted")

    subtree_sql = """
        SELECT folder_id
        FROM sb_folders
        START WITH folder_id = :folder_id
        CONNECT BY PRIOR folder_id = parent_folder_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(subtree_sql, {"folder_id": folder_id})
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                return False

            bind = ",".join([f":i{idx}" for idx, _ in enumerate(ids)])
            bind_params = {f"i{idx}": val for idx, val in enumerate(ids)}

            cur.execute(
                f"""
                UPDATE sb_folders
                SET previous_parent_folder_id = parent_folder_id
                WHERE folder_id IN ({bind})
                """,
                bind_params,
            )
            cur.execute(
                f"""
                UPDATE sb_folders
                SET is_trashed = 'Y'
                WHERE folder_id IN ({bind})
                """,
                bind_params,
            )
            cur.execute(
                "UPDATE sb_folders SET parent_folder_id = :trash_id WHERE folder_id = :folder_id",
                {"trash_id": trash_id, "folder_id": folder_id},
            )
            cur.execute(
                f"""
                UPDATE sb_files
                SET previous_folder_id = folder_id,
                    is_trashed = 'Y'
                WHERE folder_id IN ({bind})
                """,
                bind_params,
            )
            conn.commit()
            return True


def restore_folder(folder_id: int) -> bool:
    subtree_sql = """
        SELECT folder_id
        FROM sb_folders
        START WITH folder_id = :folder_id
        CONNECT BY PRIOR folder_id = parent_folder_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(subtree_sql, {"folder_id": folder_id})
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                return False

            bind = ",".join([f":i{idx}" for idx, _ in enumerate(ids)])
            bind_params = {f"i{idx}": val for idx, val in enumerate(ids)}

            cur.execute(
                """
                UPDATE sb_folders
                SET parent_folder_id = NVL(previous_parent_folder_id, parent_folder_id)
                WHERE folder_id = :folder_id
                """,
                {"folder_id": folder_id},
            )
            cur.execute(f"UPDATE sb_folders SET is_trashed='N' WHERE folder_id IN ({bind})", bind_params)
            cur.execute(f"UPDATE sb_files SET is_trashed='N' WHERE folder_id IN ({bind})", bind_params)
            conn.commit()
            return True


def purge_folder(folder_id: int) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sb_folders WHERE folder_id = :folder_id AND is_trashed = 'Y'", {"folder_id": folder_id})
            conn.commit()
            return cur.rowcount > 0


def create_file(folder_id: int, title: str, content_md: str, tags: str | None) -> int:
    title = (title or "").strip()
    if not title:
        raise ValueError("Title is required")
    if len(title) > 300:
        raise ValueError("Title too long")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            file_id_var = cur.var(int)
            try:
                cur.execute(
                    """
                    INSERT INTO sb_files (folder_id, title, content_md, tags, is_trashed)
                    VALUES (:folder_id, :title, :content_md, :tags, 'N')
                    RETURNING file_id INTO :file_id
                    """,
                    {
                        "folder_id": folder_id,
                        "title": title,
                        "content_md": content_md or "",
                        "tags": (tags or "").strip() or None,
                        "file_id": file_id_var,
                    },
                )
                conn.commit()
                return int(file_id_var.getvalue()[0])
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("File title already exists in selected folder") from exc
                raise


def get_file(file_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT file_id, folder_id, title, content_md, tags, is_trashed, created_at, updated_at
        FROM sb_files
        WHERE file_id = :file_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"file_id": file_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "file_id": int(row[0]),
                "folder_id": int(row[1]),
                "title": row[2],
                "content_md": _to_text(row[3]),
                "tags": row[4],
                "is_trashed": row[5],
                "created_at": row[6],
                "updated_at": row[7],
            }


def update_file(file_id: int, title: str, content_md: str, tags: str | None, folder_id: int | None = None) -> bool:
    title = (title or "").strip()
    if not title:
        raise ValueError("Title is required")

    updates = ["title = :title", "content_md = :content_md", "tags = :tags"]
    params: dict[str, Any] = {
        "file_id": file_id,
        "title": title,
        "content_md": content_md or "",
        "tags": (tags or "").strip() or None,
    }
    if folder_id is not None:
        updates.append("folder_id = :folder_id")
        params["folder_id"] = folder_id

    sql = f"UPDATE sb_files SET {', '.join(updates)} WHERE file_id = :file_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount > 0
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("File title already exists in selected folder") from exc
                raise


def autosave_file(file_id: int, content_md: str) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE sb_files SET content_md = :content_md WHERE file_id = :file_id", {"content_md": content_md or "", "file_id": file_id})
            conn.commit()
            return cur.rowcount > 0


def trash_file(file_id: int) -> bool:
    trash_id = ensure_trash_folder()
    sql = """
        UPDATE sb_files
        SET previous_folder_id = folder_id,
            folder_id = :trash_id,
            is_trashed = 'Y'
        WHERE file_id = :file_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"file_id": file_id, "trash_id": trash_id})
            conn.commit()
            return cur.rowcount > 0


def restore_file(file_id: int) -> bool:
    sql = """
        UPDATE sb_files
        SET folder_id = NVL(previous_folder_id, folder_id),
            is_trashed = 'N'
        WHERE file_id = :file_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"file_id": file_id})
            conn.commit()
            return cur.rowcount > 0


def purge_file(file_id: int) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sb_files WHERE file_id = :file_id AND is_trashed = 'Y'", {"file_id": file_id})
            conn.commit()
            return cur.rowcount > 0


def list_trash() -> dict[str, list[dict[str, Any]]]:
    out = {"folders": [], "files": []}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT folder_id, folder_name, updated_at
                FROM sb_folders
                WHERE is_trashed = 'Y'
                ORDER BY updated_at DESC
                """
            )
            out["folders"] = [{"folder_id": int(r[0]), "folder_name": r[1], "updated_at": r[2]} for r in cur.fetchall()]

            cur.execute(
                """
                SELECT file_id, title, updated_at
                FROM sb_files
                WHERE is_trashed = 'Y'
                ORDER BY updated_at DESC
                """
            )
            out["files"] = [{"file_id": int(r[0]), "title": r[1], "updated_at": r[2]} for r in cur.fetchall()]
    return out


def _pair(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def add_file_link(file_id: int, target_file_id: int) -> None:
    if file_id == target_file_id:
        raise ValueError("Self-link is not allowed")
    low, high = _pair(file_id, target_file_id)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sb_file_links (file_id_low, file_id_high)
                VALUES (:low, :high)
                """,
                {"low": low, "high": high},
            )
            conn.commit()


def remove_file_link(file_id: int, target_file_id: int) -> bool:
    low, high = _pair(file_id, target_file_id)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sb_file_links WHERE file_id_low = :low AND file_id_high = :high",
                {"low": low, "high": high},
            )
            conn.commit()
            return cur.rowcount > 0


def list_file_links(file_id: int) -> list[dict[str, Any]]:
    sql = """
        SELECT f.file_id, f.title
        FROM sb_file_links l
        JOIN sb_files f
          ON f.file_id = CASE WHEN l.file_id_low = :file_id THEN l.file_id_high ELSE l.file_id_low END
        WHERE l.file_id_low = :file_id OR l.file_id_high = :file_id
        ORDER BY f.title
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"file_id": file_id})
            return [{"file_id": int(r[0]), "title": r[1]} for r in cur.fetchall()]


def list_link_candidates(exclude_file_id: int) -> list[dict[str, Any]]:
    sql = """
        SELECT file_id, title
        FROM sb_files
        WHERE is_trashed = 'N' AND file_id <> :file_id
        ORDER BY updated_at DESC
        FETCH FIRST 100 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"file_id": exclude_file_id})
            return [{"file_id": int(r[0]), "title": r[1]} for r in cur.fetchall()]

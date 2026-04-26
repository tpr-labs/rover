from typing import Any
import uuid

import oracledb

from app.core.db import get_db_connection


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        return value.read() or ""
    return str(value)


def list_folders_tree() -> list[dict[str, Any]]:
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


def list_child_folders(parent_folder_id: int | None) -> list[dict[str, Any]]:
    where = "parent_folder_id IS NULL" if parent_folder_id is None else "parent_folder_id = :parent_folder_id"
    sql = f"""
        SELECT
            f.folder_id,
            f.parent_folder_id,
            f.folder_name,
            f.updated_at,
            NVL(fc.file_count, 0) AS file_count
        FROM sb_folders
        f
        LEFT JOIN (
            SELECT folder_id, COUNT(*) AS file_count
            FROM sb_files
            WHERE is_trashed = 'N'
            GROUP BY folder_id
        ) fc ON fc.folder_id = f.folder_id
        WHERE f.is_trashed = 'N' AND {where}
        ORDER BY f.folder_name
    """
    params = {} if parent_folder_id is None else {"parent_folder_id": parent_folder_id}
    out = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for folder_id, parent_id, name, updated_at, file_count in cur.fetchall():
                out.append(
                    {
                        "folder_id": int(folder_id),
                        "parent_folder_id": int(parent_id) if parent_id is not None else None,
                        "folder_name": name,
                        "updated_at": updated_at,
                        "file_count": int(file_count or 0),
                    }
                )
    return out


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
    file_sql = """
        SELECT file_id, title, tags, content_md, updated_at
        FROM sb_files
        WHERE is_trashed = 'N'
          AND (
                LOWER(title) LIKE :q
                OR LOWER(NVL(tags, '')) LIKE :q
                OR LOWER(NVL(content_md, '')) LIKE :q
              )
        ORDER BY updated_at DESC
        FETCH FIRST 100 ROWS ONLY
    """
    folder_sql = """
        SELECT folder_id, folder_name, updated_at
        FROM sb_folders
        WHERE is_trashed = 'N'
          AND LOWER(folder_name) LIKE :q
        ORDER BY updated_at DESC
        FETCH FIRST 100 ROWS ONLY
    """
    out = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(file_sql, {"q": f"%{q}%"})
            for file_id, title, tags, content_md, updated_at in cur.fetchall():
                out.append(
                    {
                        "result_type": "file",
                        "file_id": int(file_id),
                        "folder_id": None,
                        "title": title,
                        "tags": tags,
                        "content_md": _to_text(content_md),
                        "updated_at": updated_at,
                    }
                )
            cur.execute(folder_sql, {"q": f"%{q}%"})
            for folder_id, folder_name, updated_at in cur.fetchall():
                out.append(
                    {
                        "result_type": "folder",
                        "file_id": None,
                        "folder_id": int(folder_id),
                        "title": folder_name,
                        "tags": None,
                        "content_md": "",
                        "updated_at": updated_at,
                    }
                )

    out.sort(key=lambda r: r.get("updated_at") or 0, reverse=True)
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


def delete_folder(folder_id: int) -> bool:
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
                DELETE FROM sb_file_links
                WHERE file_id_low IN (SELECT file_id FROM sb_files WHERE folder_id IN ({bind}))
                   OR file_id_high IN (SELECT file_id FROM sb_files WHERE folder_id IN ({bind}))
                """,
                bind_params,
            )
            cur.execute(
                f"""
                DELETE FROM sb_files
                WHERE folder_id IN ({bind})
                """,
                bind_params,
            )

            cur.execute(
                """
                SELECT folder_id
                FROM sb_folders
                START WITH folder_id = :folder_id
                CONNECT BY PRIOR folder_id = parent_folder_id
                ORDER BY LEVEL DESC
                """,
                {"folder_id": folder_id},
            )
            for (fid,) in cur.fetchall():
                cur.execute("DELETE FROM sb_folders WHERE folder_id = :folder_id", {"folder_id": int(fid)})

            conn.commit()
            return True


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
        SELECT file_id, folder_id, title, content_md, tags, is_trashed, is_public, public_token, created_at, updated_at
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
                "is_public": row[6],
                "public_token": row[7],
                "created_at": row[8],
                "updated_at": row[9],
            }


def get_public_file_by_token(public_token: str) -> dict[str, Any] | None:
    sql = """
        SELECT file_id, folder_id, title, content_md, tags, created_at, updated_at
        FROM sb_files
        WHERE public_token = :public_token
          AND is_public = 'Y'
          AND is_trashed = 'N'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"public_token": public_token})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "file_id": int(row[0]),
                "folder_id": int(row[1]),
                "title": row[2],
                "content_md": _to_text(row[3]),
                "tags": row[4],
                "created_at": row[5],
                "updated_at": row[6],
                "is_public": "Y",
                "public_token": public_token,
            }


def is_public_content_globally_enabled() -> bool:
    sql = """
        SELECT item_value
        FROM kv_store
        WHERE item_key = 'SHOW_PUBLIC_CONTENT'
          AND LOWER(NVL(category, '')) = 'toggle'
          AND is_active = 'Y'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return False
            return (row[0] or "N").strip().upper() == "Y"


def set_file_public_state(file_id: int, make_public: bool, regenerate_token: bool = False) -> dict[str, Any] | None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if not make_public:
                cur.execute(
                    "UPDATE sb_files SET is_public = 'N', public_token = NULL WHERE file_id = :file_id",
                    {"file_id": file_id},
                )
                conn.commit()
                if cur.rowcount == 0:
                    return None
                return get_file(file_id)

            if not regenerate_token:
                cur.execute(
                    "UPDATE sb_files SET is_public = 'Y', public_token = NVL(public_token, :token) WHERE file_id = :file_id",
                    {"file_id": file_id, "token": str(uuid.uuid4())},
                )
                conn.commit()
                if cur.rowcount == 0:
                    return None
                return get_file(file_id)

            for _ in range(5):
                token = str(uuid.uuid4())
                try:
                    cur.execute(
                        "UPDATE sb_files SET is_public = 'Y', public_token = :token WHERE file_id = :file_id",
                        {"file_id": file_id, "token": token},
                    )
                    conn.commit()
                    if cur.rowcount == 0:
                        return None
                    return get_file(file_id)
                except oracledb.IntegrityError as exc:
                    err = exc.args[0]
                    if getattr(err, "code", None) == 1:
                        continue
                    raise

            raise ValueError("Unable to generate unique public token")


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


def delete_file(file_id: int) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sb_file_links WHERE file_id_low = :file_id OR file_id_high = :file_id", {"file_id": file_id})
            cur.execute("DELETE FROM sb_files WHERE file_id = :file_id", {"file_id": file_id})
            conn.commit()
            return cur.rowcount > 0


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


def get_graph_snapshot(scope: str = "subtree", root_folder_id: int | None = None) -> dict[str, Any]:
    def _load_folders(cur) -> list[dict[str, Any]]:
        if scope == "all":
            cur.execute(
                """
                SELECT folder_id, parent_folder_id, folder_name, created_at, updated_at
                FROM sb_folders
                WHERE is_trashed = 'N'
                ORDER BY folder_name
                """
            )
        else:
            if root_folder_id is None:
                return []
            cur.execute(
                """
                SELECT folder_id, parent_folder_id, folder_name, created_at, updated_at
                FROM sb_folders
                WHERE is_trashed = 'N'
                START WITH folder_id = :folder_id
                CONNECT BY PRIOR folder_id = parent_folder_id
                """,
                {"folder_id": root_folder_id},
            )

        folders = []
        for folder_id, parent_folder_id, folder_name, created_at, updated_at in cur.fetchall():
            folders.append(
                {
                    "folder_id": int(folder_id),
                    "parent_folder_id": int(parent_folder_id) if parent_folder_id is not None else None,
                    "folder_name": folder_name,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )
        return folders

    def _load_files(cur, folder_ids: list[int]) -> list[dict[str, Any]]:
        if not folder_ids:
            return []
        bind = ",".join([f":f{idx}" for idx, _ in enumerate(folder_ids)])
        params = {f"f{idx}": folder_id for idx, folder_id in enumerate(folder_ids)}
        cur.execute(
            f"""
            SELECT file_id, folder_id, title, tags, is_public, created_at, updated_at
            FROM sb_files
            WHERE is_trashed = 'N' AND folder_id IN ({bind})
            ORDER BY updated_at DESC
            """,
            params,
        )
        return [
            {
                "file_id": int(file_id),
                "folder_id": int(folder_id),
                "title": title,
                "tags": tags,
                "is_public": is_public,
                "created_at": created_at,
                "updated_at": updated_at,
            }
            for file_id, folder_id, title, tags, is_public, created_at, updated_at in cur.fetchall()
        ]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            folders = _load_folders(cur)
            folder_ids = [f["folder_id"] for f in folders]
            files = _load_files(cur, folder_ids)

            cur.execute(
                """
                SELECT l.file_id_low, l.file_id_high
                FROM sb_file_links l
                JOIN sb_files f1 ON f1.file_id = l.file_id_low AND f1.is_trashed = 'N'
                JOIN sb_files f2 ON f2.file_id = l.file_id_high AND f2.is_trashed = 'N'
                """
            )
            explicit_links = [(int(a), int(b)) for a, b in cur.fetchall()]

    file_ids = {f["file_id"] for f in files}

    nodes = []
    for folder in folders:
        nodes.append(
            {
                "id": f"folder:{folder['folder_id']}",
                "kind": "folder",
                "name": folder["folder_name"],
                "folder_id": folder["folder_id"],
                "parent_folder_id": folder["parent_folder_id"],
                "created_at": folder["created_at"],
                "updated_at": folder["updated_at"],
            }
        )

    files_by_folder: dict[int, list[dict[str, Any]]] = {}
    for file in files:
        files_by_folder.setdefault(file["folder_id"], []).append(file)
        nodes.append(
            {
                "id": f"file:{file['file_id']}",
                "kind": "file",
                "name": file["title"],
                "file_id": file["file_id"],
                "folder_id": file["folder_id"],
                "tags": file["tags"],
                "is_public": file.get("is_public", "N"),
                "created_at": file["created_at"],
                "updated_at": file["updated_at"],
            }
        )

    links = []
    explicit_pairs = set()

    for low, high in explicit_links:
        if low in file_ids and high in file_ids:
            explicit_pairs.add((min(low, high), max(low, high)))
            links.append(
                {
                    "source": f"file:{low}",
                    "target": f"file:{high}",
                    "kind": "explicit_link",
                }
            )

    for folder in folders:
        if folder["parent_folder_id"] is not None:
            links.append(
                {
                    "source": f"folder:{folder['parent_folder_id']}",
                    "target": f"folder:{folder['folder_id']}",
                    "kind": "folder_tree",
                }
            )

    root_folder_ids = [f["folder_id"] for f in folders if f.get("parent_folder_id") is None]
    if root_folder_ids:
        nodes.append(
            {
                "id": "root:all",
                "kind": "root_hub",
                "name": "Root",
                "created_at": None,
                "updated_at": None,
            }
        )
        for folder_id in root_folder_ids:
            links.append(
                {
                    "source": "root:all",
                    "target": f"folder:{folder_id}",
                    "kind": "root_hub_link",
                }
            )

    for folder_id, folder_files in files_by_folder.items():
        for file in folder_files:
            links.append(
                {
                    "source": f"folder:{folder_id}",
                    "target": f"file:{file['file_id']}",
                    "kind": "folder_contains",
                }
            )

    return {
        "scope": scope,
        "root_folder_id": root_folder_id,
        "nodes": nodes,
        "links": links,
    }

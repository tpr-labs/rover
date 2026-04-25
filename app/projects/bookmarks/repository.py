import math

import oracledb

from app.core.db import get_db_connection


def _normalize_starred(value: int | str | None) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return 1 if value == 1 else 0
    text = str(value).strip().lower()
    return 1 if text in {"1", "y", "yes", "true", "on"} else 0


def validate_bookmark_input(url: str, title: str, category: str | None, notes: str | None) -> None:
    url = (url or "").strip()
    title = (title or "").strip()
    if not url:
        raise ValueError("URL is required")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("URL must start with http:// or https://")
    if len(url) > 2000:
        raise ValueError("URL must be at most 2000 characters")
    if not title:
        raise ValueError("Title is required")
    if len(title) > 500:
        raise ValueError("Title must be at most 500 characters")
    if category and len(category) > 100:
        raise ValueError("Category must be at most 100 characters")
    if notes and len(notes) > 4000:
        raise ValueError("Notes must be at most 4000 characters")


def list_bookmarks(
    search: str | None,
    category: str | None,
    starred: str | None,
    page: int,
    page_size: int,
) -> tuple[list[dict], int, str]:
    search = (search or "").strip()
    category = (category or "").strip()
    starred = (starred or "all").strip().lower()
    if starred not in {"all", "starred", "unstarred"}:
        starred = "all"

    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}

    if search:
        where.append(
            "(" 
            "LOWER(url) LIKE :search OR "
            "LOWER(title) LIKE :search OR "
            "LOWER(NVL(category, '')) LIKE :search OR "
            "LOWER(NVL(DBMS_LOB.SUBSTR(notes, 1000, 1), '')) LIKE :search"
            ")"
        )
        params["search"] = f"%{search.lower()}%"

    if category:
        where.append("LOWER(NVL(category, '')) = :category")
        params["category"] = category.lower()

    if starred == "starred":
        where.append("starred = 1")
    elif starred == "unstarred":
        where.append("starred = 0")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    count_sql = f"SELECT COUNT(*) FROM bookmarks {where_sql}"
    list_sql = f"""
        SELECT bookmark_id, url, title, category, starred, notes, created_at, updated_at
        FROM bookmarks
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
                    "bookmark_id": int(r[0]),
                    "url": r[1],
                    "title": r[2],
                    "category": r[3],
                    "starred": int(r[4] or 0),
                    "notes": r[5],
                    "created_at": r[6],
                    "updated_at": r[7],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, starred


def get_bookmark(bookmark_id: int) -> dict | None:
    sql = """
        SELECT bookmark_id, url, title, category, starred, notes, created_at, updated_at
        FROM bookmarks
        WHERE bookmark_id = :bookmark_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"bookmark_id": bookmark_id})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "bookmark_id": int(row[0]),
                "url": row[1],
                "title": row[2],
                "category": row[3],
                "starred": int(row[4] or 0),
                "notes": row[5],
                "created_at": row[6],
                "updated_at": row[7],
            }


def create_bookmark(url: str, title: str, category: str | None, starred: int | str | None, notes: str | None) -> int:
    validate_bookmark_input(url, title, category, notes)
    sql = """
        INSERT INTO bookmarks (url, title, category, starred, notes)
        VALUES (:url, :title, :category, :starred, :notes)
        RETURNING bookmark_id INTO :bookmark_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            try:
                cur.execute(
                    sql,
                    {
                        "url": url.strip(),
                        "title": title.strip(),
                        "category": (category or "").strip() or None,
                        "starred": _normalize_starred(starred),
                        "notes": (notes or "").strip() or None,
                        "bookmark_id": out_id,
                    },
                )
                conn.commit()
                return int(out_id.getvalue()[0])
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Bookmark URL already exists") from exc
                raise


def update_bookmark(
    bookmark_id: int,
    url: str,
    title: str,
    category: str | None,
    starred: int | str | None,
    notes: str | None,
) -> bool:
    validate_bookmark_input(url, title, category, notes)
    sql = """
        UPDATE bookmarks
        SET url = :url,
            title = :title,
            category = :category,
            starred = :starred,
            notes = :notes
        WHERE bookmark_id = :bookmark_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    sql,
                    {
                        "bookmark_id": bookmark_id,
                        "url": url.strip(),
                        "title": title.strip(),
                        "category": (category or "").strip() or None,
                        "starred": _normalize_starred(starred),
                        "notes": (notes or "").strip() or None,
                    },
                )
                conn.commit()
                return cur.rowcount > 0
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    raise ValueError("Bookmark URL already exists") from exc
                raise


def delete_bookmark(bookmark_id: int) -> bool:
    sql = "DELETE FROM bookmarks WHERE bookmark_id = :bookmark_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"bookmark_id": bookmark_id})
            conn.commit()
            return cur.rowcount > 0


def switch_bookmark_starred(bookmark_id: int, desired_starred: int | str | None) -> bool:
    sql = """
        UPDATE bookmarks
        SET starred = :starred
        WHERE bookmark_id = :bookmark_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "bookmark_id": bookmark_id,
                    "starred": _normalize_starred(desired_starred),
                },
            )
            conn.commit()
            return cur.rowcount > 0

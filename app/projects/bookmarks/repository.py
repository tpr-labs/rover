import math
from typing import Any

import oracledb

from app.core.db import get_db_connection


def _is_missing_table_error(exc: oracledb.DatabaseError) -> bool:
    err = exc.args[0] if exc.args else None
    return getattr(err, "code", None) == 942


def _normalize_starred(value: int | str | None) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return 1 if value == 1 else 0
    text = str(value).strip().lower()
    return 1 if text in {"1", "y", "yes", "true", "on"} else 0


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        data = value.read()
        return data or ""
    return str(value)


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
            "LOWER(b.url) LIKE :search OR "
            "LOWER(b.title) LIKE :search OR "
            "LOWER(NVL(b.category, '')) LIKE :search OR "
            "LOWER(NVL(DBMS_LOB.SUBSTR(b.notes, 1000, 1), '')) LIKE :search"
            ")"
        )
        params["search"] = f"%{search.lower()}%"

    if category:
        where.append("LOWER(NVL(b.category, '')) = :category")
        params["category"] = category.lower()

    if starred == "starred":
        where.append("b.starred = 1")
    elif starred == "unstarred":
        where.append("b.starred = 0")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    count_sql = f"SELECT COUNT(*) FROM bookmarks b {where_sql}"
    list_sql = f"""
        SELECT b.bookmark_id,
               b.url,
               b.title,
               b.category,
               b.starred,
               b.notes,
               b.created_at,
               b.updated_at,
               NVL(sc.card_count, 0) AS study_card_count,
               sj.status AS study_card_job_status
        FROM bookmarks b
        LEFT JOIN (
            SELECT bookmark_id, COUNT(*) AS card_count
            FROM bookmark_study_cards
            GROUP BY bookmark_id
        ) sc ON sc.bookmark_id = b.bookmark_id
        LEFT JOIN (
            SELECT bookmark_id, status
            FROM (
                SELECT j.bookmark_id,
                       j.status,
                       ROW_NUMBER() OVER (PARTITION BY j.bookmark_id ORDER BY j.created_at DESC, j.job_id DESC) AS rn
                FROM bookmark_study_card_jobs j
            )
            WHERE rn = 1
        ) sj ON sj.bookmark_id = b.bookmark_id
        {where_sql}
        ORDER BY b.updated_at DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """
    fallback_list_sql = f"""
        SELECT b.bookmark_id, b.url, b.title, b.category, b.starred, b.notes, b.created_at, b.updated_at
        FROM bookmarks b
        {where_sql}
        ORDER BY b.updated_at DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = int(cur.fetchone()[0])

            q_params = dict(params)
            q_params.update({"offset": offset, "limit": page_size})
            try:
                cur.execute(list_sql, q_params)
                rows = [
                    {
                        "bookmark_id": int(r[0]),
                        "url": r[1],
                        "title": r[2],
                        "category": r[3],
                        "starred": int(r[4] or 0),
                        "notes": _coerce_text(r[5]),
                        "created_at": r[6],
                        "updated_at": r[7],
                        "study_card_count": int(r[8] or 0),
                        "study_card_job_status": r[9],
                    }
                    for r in cur.fetchall()
                ]
            except oracledb.DatabaseError as exc:
                if not _is_missing_table_error(exc):
                    raise
                cur.execute(fallback_list_sql, q_params)
                rows = [
                    {
                        "bookmark_id": int(r[0]),
                        "url": r[1],
                        "title": r[2],
                        "category": r[3],
                        "starred": int(r[4] or 0),
                        "notes": _coerce_text(r[5]),
                        "created_at": r[6],
                        "updated_at": r[7],
                        "study_card_count": 0,
                        "study_card_job_status": None,
                    }
                    for r in cur.fetchall()
                ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, starred


def get_bookmark(bookmark_id: int) -> dict | None:
    sql = """
        SELECT b.bookmark_id,
               b.url,
               b.title,
               b.category,
               b.starred,
               b.notes,
               b.created_at,
               b.updated_at,
               (
                   SELECT COUNT(*)
                   FROM bookmark_study_cards sc
                   WHERE sc.bookmark_id = b.bookmark_id
               ) AS study_card_count,
               (
                   SELECT status
                   FROM (
                       SELECT j.status,
                              ROW_NUMBER() OVER (ORDER BY j.created_at DESC, j.job_id DESC) AS rn
                       FROM bookmark_study_card_jobs j
                       WHERE j.bookmark_id = b.bookmark_id
                   )
                   WHERE rn = 1
               ) AS study_card_job_status
        FROM bookmarks b
        WHERE b.bookmark_id = :bookmark_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"bookmark_id": bookmark_id})
                row = cur.fetchone()
            except oracledb.DatabaseError as exc:
                if not _is_missing_table_error(exc):
                    raise
                cur.execute(
                    """
                    SELECT bookmark_id, url, title, category, starred, notes, created_at, updated_at
                    FROM bookmarks
                    WHERE bookmark_id = :bookmark_id
                    """,
                    {"bookmark_id": bookmark_id},
                )
                base = cur.fetchone()
                if not base:
                    return None
                return {
                    "bookmark_id": int(base[0]),
                    "url": base[1],
                    "title": base[2],
                    "category": base[3],
                    "starred": int(base[4] or 0),
                    "notes": _coerce_text(base[5]),
                    "created_at": base[6],
                    "updated_at": base[7],
                    "study_card_count": 0,
                    "study_card_job_status": None,
                }
            if not row:
                return None
            return {
                "bookmark_id": int(row[0]),
                "url": row[1],
                "title": row[2],
                "category": row[3],
                "starred": int(row[4] or 0),
                "notes": _coerce_text(row[5]),
                "created_at": row[6],
                "updated_at": row[7],
                "study_card_count": int(row[8] or 0),
                "study_card_job_status": row[9],
            }


def get_bookmark_study_cards_max(default_value: int = 10) -> int:
    sql = """
        SELECT additional_info, item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(NVL(category, '')) = 'config'
          AND is_active = 'Y'
        ORDER BY updated_at DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": "BOOKMARK_STUDY_CARDS_MAX"})
            row = cur.fetchone()

    if not row:
        return default_value

    raw = (str(row[1] or "").strip() or str(row[0] or "").strip() or "")
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return default_value
    return max(1, min(20, val))


def count_study_cards(bookmark_id: int) -> int:
    sql = "SELECT COUNT(*) FROM bookmark_study_cards WHERE bookmark_id = :bookmark_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"bookmark_id": bookmark_id})
                row = cur.fetchone()
                return int(row[0] or 0) if row else 0
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return 0
                raise


def list_study_cards(bookmark_id: int) -> list[dict[str, Any]]:
    sql = """
        SELECT card_id, bookmark_id, card_no, question, answer, source_excerpt, created_at, updated_at
        FROM bookmark_study_cards
        WHERE bookmark_id = :bookmark_id
        ORDER BY card_no, card_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"bookmark_id": bookmark_id})
                return [
                    {
                        "card_id": int(r[0]),
                        "bookmark_id": int(r[1]),
                        "card_no": int(r[2]),
                        "question": r[3],
                        "answer": r[4],
                        "source_excerpt": r[5],
                        "created_at": r[6],
                        "updated_at": r[7],
                    }
                    for r in cur.fetchall()
                ]
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return []
                raise


def replace_study_cards(bookmark_id: int, cards: list[dict[str, str]]) -> int:
    if not cards:
        raise ValueError("No study cards to save")

    delete_sql = "DELETE FROM bookmark_study_cards WHERE bookmark_id = :bookmark_id"
    insert_sql = """
        INSERT INTO bookmark_study_cards (bookmark_id, card_no, question, answer, source_excerpt)
        VALUES (:bookmark_id, :card_no, :question, :answer, :source_excerpt)
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(delete_sql, {"bookmark_id": bookmark_id})
                inserted = 0
                for idx, card in enumerate(cards, start=1):
                    question = (card.get("question") or "").strip()
                    answer = (card.get("answer") or "").strip()
                    excerpt = (card.get("source_excerpt") or "").strip() or None
                    if not question or not answer:
                        continue
                    cur.execute(
                        insert_sql,
                        {
                            "bookmark_id": bookmark_id,
                            "card_no": idx,
                            "question": question[:500],
                            "answer": answer,
                            "source_excerpt": excerpt,
                        },
                    )
                    inserted += 1
                conn.commit()
                return inserted
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    raise ValueError("Study cards table not found. Please run sql/bookmark_study_cards.sql") from exc
                raise


def create_study_card_job(bookmark_id: int) -> int:
    sql = """
        INSERT INTO bookmark_study_card_jobs (bookmark_id, status)
        VALUES (:bookmark_id, 'QUEUED')
        RETURNING job_id INTO :job_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            try:
                cur.execute(sql, {"bookmark_id": bookmark_id, "job_id": out_id})
                conn.commit()
                return int(out_id.getvalue()[0])
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    raise ValueError("Study card jobs table not found. Please run sql/bookmark_study_card_jobs.sql") from exc
                raise


def get_latest_study_card_job(bookmark_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT job_id, bookmark_id, status, error_message, created_at, started_at, finished_at, updated_at
        FROM bookmark_study_card_jobs
        WHERE bookmark_id = :bookmark_id
        ORDER BY created_at DESC, job_id DESC
        FETCH FIRST 1 ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"bookmark_id": bookmark_id})
                row = cur.fetchone()
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return None
                raise
            if not row:
                return None
            return {
                "job_id": int(row[0]),
                "bookmark_id": int(row[1]),
                "status": row[2],
                "error_message": row[3],
                "created_at": row[4],
                "started_at": row[5],
                "finished_at": row[6],
                "updated_at": row[7],
            }


def get_study_card_job_by_id(job_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT job_id, bookmark_id, status, error_message, created_at, started_at, finished_at, updated_at
        FROM bookmark_study_card_jobs
        WHERE job_id = :job_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"job_id": job_id})
                row = cur.fetchone()
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return None
                raise
            if not row:
                return None
            return {
                "job_id": int(row[0]),
                "bookmark_id": int(row[1]),
                "status": row[2],
                "error_message": _coerce_text(row[3])[:2000] if row[3] is not None else None,
                "created_at": row[4],
                "started_at": row[5],
                "finished_at": row[6],
                "updated_at": row[7],
            }


def get_study_card_job_detail(job_id: int) -> dict[str, Any] | None:
    sql = """
        SELECT j.job_id,
               j.bookmark_id,
               j.status,
               j.error_message,
               j.created_at,
               j.started_at,
               j.finished_at,
               j.updated_at,
               b.title,
               b.url
        FROM bookmark_study_card_jobs j
        LEFT JOIN bookmarks b ON b.bookmark_id = j.bookmark_id
        WHERE j.job_id = :job_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"job_id": job_id})
                row = cur.fetchone()
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return None
                raise
            if not row:
                return None
            return {
                "job_id": int(row[0]),
                "bookmark_id": int(row[1]),
                "status": row[2],
                "error_message": _coerce_text(row[3])[:2000] if row[3] is not None else None,
                "created_at": row[4],
                "started_at": row[5],
                "finished_at": row[6],
                "updated_at": row[7],
                "bookmark_title": row[8],
                "bookmark_url": row[9],
            }


def list_study_card_jobs(page: int, page_size: int, bookmark_id: int | None = None) -> tuple[list[dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 20), 100))
    offset = (page - 1) * page_size

    where_sql = ""
    params: dict[str, Any] = {}
    if bookmark_id is not None:
        where_sql = "WHERE j.bookmark_id = :bookmark_id"
        params["bookmark_id"] = int(bookmark_id)

    count_sql = f"SELECT COUNT(*) FROM bookmark_study_card_jobs j {where_sql}"
    list_sql = f"""
        SELECT j.job_id,
               j.bookmark_id,
               j.status,
               j.error_message,
               j.created_at,
               j.started_at,
               j.finished_at,
               j.updated_at,
               b.title,
               b.url
        FROM bookmark_study_card_jobs j
        LEFT JOIN bookmarks b ON b.bookmark_id = j.bookmark_id
        {where_sql}
        ORDER BY j.created_at DESC, j.job_id DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(count_sql, params)
                total = int((cur.fetchone() or [0])[0] or 0)

                q_params = dict(params)
                q_params.update({"offset": offset, "limit": page_size})
                cur.execute(list_sql, q_params)
                rows = [
                    {
                        "job_id": int(r[0]),
                        "bookmark_id": int(r[1]),
                        "status": r[2],
                        "error_message": _coerce_text(r[3])[:2000] if r[3] is not None else None,
                        "created_at": r[4],
                        "started_at": r[5],
                        "finished_at": r[6],
                        "updated_at": r[7],
                        "bookmark_title": r[8],
                        "bookmark_url": r[9],
                    }
                    for r in cur.fetchall()
                ]
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return [], 1
                raise

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages


def claim_study_card_job(job_id: int) -> bool:
    sql = """
        UPDATE bookmark_study_card_jobs
        SET status = 'RUNNING',
            started_at = SYSTIMESTAMP,
            error_message = NULL
        WHERE job_id = :job_id
          AND status = 'QUEUED'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"job_id": job_id})
                conn.commit()
                return cur.rowcount > 0
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return False
                raise


def complete_study_card_job(job_id: int) -> None:
    sql = """
        UPDATE bookmark_study_card_jobs
        SET status = 'COMPLETED',
            finished_at = SYSTIMESTAMP,
            error_message = NULL
        WHERE job_id = :job_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"job_id": job_id})
                conn.commit()
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return
                raise


def fail_study_card_job(job_id: int, error_message: str) -> None:
    sql = """
        UPDATE bookmark_study_card_jobs
        SET status = 'FAILED',
            finished_at = SYSTIMESTAMP,
            error_message = :error_message
        WHERE job_id = :job_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, {"job_id": job_id, "error_message": (error_message or "")[:2000]})
                conn.commit()
            except oracledb.DatabaseError as exc:
                if _is_missing_table_error(exc):
                    return
                raise


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


def count_uncategorized_bookmarks() -> int:
    sql = """
        SELECT COUNT(*)
        FROM bookmarks
        WHERE category IS NULL
           OR TRIM(category) = ''
           OR LOWER(TRIM(category)) IN ('none', 'null')
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def list_uncategorized_bookmarks(limit: int = 10) -> list[dict]:
    cap = max(1, min(int(limit), 10))
    sql = """
        SELECT bookmark_id, url, title, category
        FROM bookmarks
        WHERE category IS NULL
           OR TRIM(category) = ''
           OR LOWER(TRIM(category)) IN ('none', 'null')
        ORDER BY updated_at DESC
        FETCH FIRST :limit ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": cap})
            return [
                {
                    "bookmark_id": int(r[0]),
                    "url": r[1],
                    "title": r[2],
                    "category": r[3],
                }
                for r in cur.fetchall()
            ]


def update_bookmark_category(bookmark_id: int, category: str) -> bool:
    normalized = (category or "").strip()
    if not normalized:
        raise ValueError("Category is required")
    if len(normalized) > 100:
        raise ValueError("Category must be at most 100 characters")

    sql = "UPDATE bookmarks SET category = :category WHERE bookmark_id = :bookmark_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"bookmark_id": bookmark_id, "category": normalized})
            conn.commit()
            return cur.rowcount > 0

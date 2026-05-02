import math
from typing import Any

from app.core.db import get_db_connection


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "read"):
        data = value.read()
        return data or ""
    return str(value)


def _normalize_status(value: str | None) -> str:
    status = (value or "all").strip().upper()
    if status not in {"ALL", "SENT", "FAILED"}:
        return "ALL"
    return status


def validate_message_text(message_text: str) -> str:
    text = (message_text or "").strip()
    if not text:
        raise ValueError("Message text is required")
    if len(text) > 4096:
        raise ValueError("Message text must be at most 4096 characters")
    return text


def list_messages(
    search: str | None,
    status: str | None,
    show_hidden: bool,
    page: int,
    page_size: int,
) -> tuple[list[dict], int, str, bool]:
    search = (search or "").strip().lower()
    status = _normalize_status(status)
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 20), 100))
    offset = (page - 1) * page_size

    where = []
    params: dict[str, object] = {}

    if not show_hidden:
        where.append("m.is_hidden = 'N'")

    if status != "ALL":
        where.append("m.status = :status")
        params["status"] = status

    if search:
        where.append("(LOWER(DBMS_LOB.SUBSTR(m.message_text, 2000, 1)) LIKE :search OR LOWER(NVL(m.telegram_chat_id, '')) LIKE :search)")
        params["search"] = f"%{search}%"

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    count_sql = f"SELECT COUNT(*) FROM messenger_messages m {where_sql}"
    list_sql = f"""
        SELECT m.message_id,
               m.message_text,
               m.status,
               m.telegram_message_id,
               m.telegram_chat_id,
               m.http_status,
               m.error_message,
               m.is_hidden,
               m.resend_of_message_id,
               m.created_at,
               m.updated_at
        FROM messenger_messages m
        {where_sql}
        ORDER BY m.created_at DESC, m.message_id DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = int((cur.fetchone() or [0])[0] or 0)

            q_params = dict(params)
            q_params.update({"offset": offset, "limit": page_size})
            cur.execute(list_sql, q_params)
            rows = [
                {
                    "message_id": int(r[0]),
                    "message_text": _coerce_text(r[1]),
                    "status": r[2],
                    "telegram_message_id": r[3],
                    "telegram_chat_id": r[4],
                    "http_status": int(r[5]) if r[5] is not None else None,
                    "error_message": _coerce_text(r[6]) if r[6] is not None else None,
                    "is_hidden": r[7],
                    "resend_of_message_id": int(r[8]) if r[8] is not None else None,
                    "created_at": r[9],
                    "updated_at": r[10],
                }
                for r in cur.fetchall()
            ]

    total_pages = max(1, math.ceil(total / page_size))
    return rows, total_pages, status, show_hidden


def get_message(message_id: int) -> dict | None:
    sql = """
        SELECT message_id,
               message_text,
               status,
               telegram_message_id,
               telegram_chat_id,
               http_status,
               response_payload,
               error_message,
               is_hidden,
               resend_of_message_id,
               created_at,
               updated_at
        FROM messenger_messages
        WHERE message_id = :message_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"message_id": int(message_id)})
            row = cur.fetchone()
            if not row:
                return None
            return {
                "message_id": int(row[0]),
                "message_text": _coerce_text(row[1]),
                "status": row[2],
                "telegram_message_id": row[3],
                "telegram_chat_id": row[4],
                "http_status": int(row[5]) if row[5] is not None else None,
                "response_payload": _coerce_text(row[6]),
                "error_message": _coerce_text(row[7]) if row[7] is not None else None,
                "is_hidden": row[8],
                "resend_of_message_id": int(row[9]) if row[9] is not None else None,
                "created_at": row[10],
                "updated_at": row[11],
            }


def create_message_record(
    message_text: str,
    status: str,
    telegram_message_id: str | None,
    telegram_chat_id: str | None,
    http_status: int | None,
    response_payload: str | None,
    error_message: str | None,
    resend_of_message_id: int | None = None,
) -> int:
    text = validate_message_text(message_text)
    st = (status or "FAILED").strip().upper()
    if st not in {"SENT", "FAILED"}:
        raise ValueError("Invalid message status")

    sql = """
        INSERT INTO messenger_messages (
            message_text,
            status,
            telegram_message_id,
            telegram_chat_id,
            http_status,
            response_payload,
            error_message,
            resend_of_message_id
        )
        VALUES (
            :message_text,
            :status,
            :telegram_message_id,
            :telegram_chat_id,
            :http_status,
            :response_payload,
            :error_message,
            :resend_of_message_id
        )
        RETURNING message_id INTO :message_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            cur.execute(
                sql,
                {
                    "message_text": text,
                    "status": st,
                    "telegram_message_id": (telegram_message_id or "").strip() or None,
                    "telegram_chat_id": (telegram_chat_id or "").strip() or None,
                    "http_status": int(http_status) if http_status is not None else None,
                    "response_payload": (response_payload or "").strip() or None,
                    "error_message": (error_message or "").strip()[:4000] or None,
                    "resend_of_message_id": int(resend_of_message_id) if resend_of_message_id is not None else None,
                    "message_id": out_id,
                },
            )
            conn.commit()
            return int(out_id.getvalue()[0])


def hide_message(message_id: int) -> bool:
    sql = "UPDATE messenger_messages SET is_hidden = 'Y' WHERE message_id = :message_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"message_id": int(message_id)})
            conn.commit()
            return cur.rowcount > 0


def unhide_message(message_id: int) -> bool:
    sql = "UPDATE messenger_messages SET is_hidden = 'N' WHERE message_id = :message_id"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"message_id": int(message_id)})
            conn.commit()
            return cur.rowcount > 0

import math
from typing import Any

import oracledb

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


def _normalize_direction(value: str | None) -> str:
    direction = (value or "OUTBOUND").strip().upper()
    if direction not in {"OUTBOUND", "INBOUND"}:
        return "OUTBOUND"
    return direction


def _normalize_processing_status(value: str | None) -> str | None:
    if value is None:
        return None
    status = (value or "").strip().upper()
    if status not in {"NEW", "PROCESSED", "FAILED"}:
        return None
    return status


def _normalize_message_type(value: str | None) -> str | None:
    if value is None:
        return None
    msg_type = (value or "").strip().upper()
    if msg_type not in {"BOOKMARK", "TRANSACTION", "UNCATEGORIZED"}:
        return None
    return msg_type


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
               m.direction,
               m.processing_status,
               m.message_type,
               m.processed_at,
               m.bookmark_id,
               m.transaction_id,
               m.telegram_message_id,
               m.telegram_update_id,
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
                    "direction": r[3],
                    "processing_status": r[4],
                    "message_type": r[5],
                    "processed_at": r[6],
                    "bookmark_id": int(r[7]) if r[7] is not None else None,
                    "transaction_id": int(r[8]) if r[8] is not None else None,
                    "telegram_message_id": r[9],
                    "telegram_update_id": int(r[10]) if r[10] is not None else None,
                    "telegram_chat_id": r[11],
                    "http_status": int(r[12]) if r[12] is not None else None,
                    "error_message": _coerce_text(r[13]) if r[13] is not None else None,
                    "is_hidden": r[14],
                    "resend_of_message_id": int(r[15]) if r[15] is not None else None,
                    "created_at": r[16],
                    "updated_at": r[17],
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
               direction,
               processing_status,
               message_type,
               processed_at,
               bookmark_id,
               transaction_id,
               telegram_message_id,
               telegram_update_id,
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
                "direction": row[3],
                "processing_status": row[4],
                "message_type": row[5],
                "processed_at": row[6],
                "bookmark_id": int(row[7]) if row[7] is not None else None,
                "transaction_id": int(row[8]) if row[8] is not None else None,
                "telegram_message_id": row[9],
                "telegram_update_id": int(row[10]) if row[10] is not None else None,
                "telegram_chat_id": row[11],
                "http_status": int(row[12]) if row[12] is not None else None,
                "response_payload": _coerce_text(row[13]),
                "error_message": _coerce_text(row[14]) if row[14] is not None else None,
                "is_hidden": row[15],
                "resend_of_message_id": int(row[16]) if row[16] is not None else None,
                "created_at": row[17],
                "updated_at": row[18],
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
            direction,
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
            'OUTBOUND',
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


def get_last_inbound_update_id(chat_id: str | None = None) -> int:
    where_chat = "AND telegram_chat_id = :chat_id" if (chat_id or "").strip() else ""
    sql = f"""
        SELECT NVL(MAX(telegram_update_id), 0)
        FROM messenger_messages
        WHERE direction = 'INBOUND'
          AND telegram_update_id IS NOT NULL
          {where_chat}
    """
    params = {"chat_id": (chat_id or "").strip()} if where_chat else {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int((row or [0])[0] or 0)


def create_inbound_message_record(
    message_text: str,
    telegram_update_id: int,
    telegram_message_id: str | None,
    telegram_chat_id: str | None,
    response_payload: str | None = None,
) -> int | None:
    text = validate_message_text(message_text)
    sql = """
        INSERT INTO messenger_messages (
            message_text,
            status,
            direction,
            processing_status,
            telegram_update_id,
            telegram_message_id,
            telegram_chat_id,
            response_payload,
            is_hidden
        )
        VALUES (
            :message_text,
            'SENT',
            'INBOUND',
            'NEW',
            :telegram_update_id,
            :telegram_message_id,
            :telegram_chat_id,
            :response_payload,
            'N'
        )
        RETURNING message_id INTO :message_id
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            out_id = cur.var(int)
            try:
                cur.execute(
                    sql,
                    {
                        "message_text": text,
                        "telegram_update_id": int(telegram_update_id),
                        "telegram_message_id": (telegram_message_id or "").strip() or None,
                        "telegram_chat_id": (telegram_chat_id or "").strip() or None,
                        "response_payload": (response_payload or "").strip() or None,
                        "message_id": out_id,
                    },
                )
                conn.commit()
                return int(out_id.getvalue()[0])
            except oracledb.IntegrityError as exc:
                err = exc.args[0]
                if getattr(err, "code", None) == 1:
                    # Duplicate telegram_update_id (already fetched)
                    return None
                raise


def list_new_inbound_messages(limit: int = 100) -> list[dict]:
    cap = max(1, min(int(limit), 500))
    sql = """
        SELECT message_id,
               message_text,
               telegram_update_id,
               telegram_message_id,
               telegram_chat_id,
               created_at
        FROM messenger_messages
        WHERE direction = 'INBOUND'
          AND NVL(processing_status, 'NEW') = 'NEW'
        ORDER BY created_at ASC, message_id ASC
        FETCH FIRST :limit ROWS ONLY
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"limit": cap})
            return [
                {
                    "message_id": int(r[0]),
                    "message_text": _coerce_text(r[1]),
                    "telegram_update_id": int(r[2]) if r[2] is not None else None,
                    "telegram_message_id": r[3],
                    "telegram_chat_id": r[4],
                    "created_at": r[5],
                }
                for r in cur.fetchall()
            ]


def mark_inbound_processed(
    message_id: int,
    message_type: str,
    bookmark_id: int | None = None,
    transaction_id: int | None = None,
) -> bool:
    normalized_type = _normalize_message_type(message_type)
    if not normalized_type:
        raise ValueError("Invalid message type")

    sql = """
        UPDATE messenger_messages
        SET processing_status = 'PROCESSED',
            message_type = :message_type,
            processed_at = SYSTIMESTAMP,
            bookmark_id = :bookmark_id,
            transaction_id = :transaction_id,
            error_message = NULL
        WHERE message_id = :message_id
          AND direction = 'INBOUND'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "message_id": int(message_id),
                    "message_type": normalized_type,
                    "bookmark_id": int(bookmark_id) if bookmark_id is not None else None,
                    "transaction_id": int(transaction_id) if transaction_id is not None else None,
                },
            )
            conn.commit()
            return cur.rowcount > 0


def mark_inbound_failed(message_id: int, error_message: str) -> bool:
    sql = """
        UPDATE messenger_messages
        SET processing_status = 'FAILED',
            processed_at = SYSTIMESTAMP,
            error_message = :error_message
        WHERE message_id = :message_id
          AND direction = 'INBOUND'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "message_id": int(message_id),
                    "error_message": (error_message or "").strip()[:4000] or "Processing failed",
                },
            )
            conn.commit()
            return cur.rowcount > 0


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


def hide_all_messages() -> int:
    sql = "UPDATE messenger_messages SET is_hidden = 'Y' WHERE is_hidden = 'N'"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            return int(cur.rowcount or 0)

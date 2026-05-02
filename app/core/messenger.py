import json
from typing import Any

import requests

from app.core.db import get_db_connection


TELEGRAM_SEND_TOGGLE_KEY = "ALLOW_TELEGRAM_SEND"
TELEGRAM_BOT_TOKEN_KV = "TELEGRAM_BOT_TOKEN"
TELEGRAM_DEFAULT_CHAT_ID_KV = "TELEGRAM_DEFAULT_CHAT_ID"


def _get_kv_pair(item_key: str) -> tuple[str | None, str | None]:
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
                return None, None
            return ((row[0] or "").strip() or None, (row[1] or "").strip() or None)


def is_telegram_send_allowed() -> bool:
    sql = """
        SELECT item_value
        FROM kv_store
        WHERE item_key = :item_key
          AND LOWER(NVL(category, '')) = 'toggle'
          AND is_active = 'Y'
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"item_key": TELEGRAM_SEND_TOGGLE_KEY})
            row = cur.fetchone()
            if not row:
                return False
            return (row[0] or "N").strip().upper() == "Y"


def get_telegram_bot_token() -> str:
    additional_info, item_value = _get_kv_pair(TELEGRAM_BOT_TOKEN_KV)
    token = (item_value or additional_info or "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN is missing")
    return token


def get_telegram_default_chat_id() -> str:
    additional_info, item_value = _get_kv_pair(TELEGRAM_DEFAULT_CHAT_ID_KV)
    chat_id = (item_value or additional_info or "").strip()
    if not chat_id:
        raise ValueError("TELEGRAM_DEFAULT_CHAT_ID is missing")
    return chat_id


def send_telegram_text(
    message_text: str,
    chat_id: str | None = None,
    parse_mode: str | None = None,
) -> dict[str, Any]:
    text = (message_text or "").strip()
    if not text:
        raise ValueError("Message text is required")
    if len(text) > 4096:
        raise ValueError("Message text must be at most 4096 characters")

    if not is_telegram_send_allowed():
        raise ValueError("Telegram send is disabled by toggle ALLOW_TELEGRAM_SEND")

    token = get_telegram_bot_token()
    target_chat_id = (chat_id or "").strip() or get_telegram_default_chat_id()

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": text,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        response = requests.post(url, json=payload, timeout=(8, 30))
        status_code = int(response.status_code)
        body_text = response.text or ""
    except requests.RequestException as exc:
        return {
            "ok": False,
            "http_status": None,
            "response_payload": "",
            "telegram_message_id": None,
            "telegram_chat_id": target_chat_id,
            "error_message": str(exc),
        }

    telegram_message_id = None
    telegram_chat_id = target_chat_id
    error_message = None
    ok = False

    try:
        body_json = response.json()
        ok = bool(body_json.get("ok")) and 200 <= status_code < 300
        result = body_json.get("result") or {}
        telegram_message_id = result.get("message_id")
        chat_obj = result.get("chat") or {}
        if chat_obj.get("id") is not None:
            telegram_chat_id = str(chat_obj.get("id"))
        if not ok:
            error_message = str(body_json.get("description") or "Telegram send failed")
        body_text = json.dumps(body_json, ensure_ascii=False)
    except Exception:
        ok = 200 <= status_code < 300
        if not ok:
            error_message = f"Telegram send failed with status {status_code}"

    return {
        "ok": ok,
        "http_status": status_code,
        "response_payload": body_text,
        "telegram_message_id": str(telegram_message_id) if telegram_message_id is not None else None,
        "telegram_chat_id": str(telegram_chat_id) if telegram_chat_id is not None else None,
        "error_message": error_message,
    }

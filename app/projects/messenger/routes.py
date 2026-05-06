from datetime import date, datetime
import re

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from app.core.messenger import fetch_telegram_updates, get_telegram_default_chat_id, send_telegram_text
from app.projects.api.repository import extract_api_key_from_request, find_active_api_key_match, get_api_key_header_name
from app.projects.bookmarks.repository import create_bookmark, get_bookmark_by_url
from app.projects.ft.repository import create_transaction
from .repository import (
    create_inbound_message_record,
    create_message_record,
    get_last_inbound_update_id,
    get_message,
    hide_message,
    hide_all_messages,
    list_messages,
    list_new_inbound_messages,
    mark_inbound_failed,
    mark_inbound_processed,
    unhide_message,
    validate_message_text,
)

messenger_bp = Blueprint("messenger", __name__)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def _classify_message(text: str) -> str:
    value = (text or "").strip()
    lower = value.lower()
    bookmark_url, _ = _extract_bookmark_payload(value)
    if bookmark_url:
        return "BOOKMARK"
    if lower.startswith("spent") or lower.startswith("paid"):
        return "TRANSACTION"
    return "UNCATEGORIZED"


def _extract_bookmark_payload(raw_text: str) -> tuple[str | None, str]:
    text = (raw_text or "").strip()
    if not text:
        return None, ""

    match = _URL_RE.search(text)
    if not match:
        return None, ""

    url = (match.group(0) or "").strip().rstrip(".,);]!?\"")
    before = text[: match.start()].strip()
    title = ""
    if before:
        title = before.splitlines()[0].strip()
    if not title:
        title = url
    return (url or None), title


def _create_transaction_from_message(raw_text: str) -> int:
    text = (raw_text or "").strip()
    amount_match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    amount = abs(float(amount_match.group(1))) if amount_match else 0.0
    if amount <= 0:
        raise ValueError("Transaction amount not found")

    lower = text.lower()
    category = "uncategorized"
    if lower.startswith("spent"):
        remainder = text[5:].strip(" :-")
        first_word = re.split(r"\s+", remainder)[0] if remainder else ""
        cleaned = re.sub(r"[^A-Za-z]", "", first_word).lower()
        if cleaned:
            category = cleaned
    elif lower.startswith("paid"):
        remainder = text[4:].strip(" :-")
        first_word = re.split(r"\s+", remainder)[0] if remainder else ""
        cleaned = re.sub(r"[^A-Za-z]", "", first_word).lower()
        if cleaned:
            category = cleaned

    return create_transaction(
        raw_text=text,
        amount=amount,
        direction="EXPENSE",
        tx_date=None,
        category=category,
        description=text,
        account_id=None,
        status="PENDING",
        is_active="Y",
    )


def _humanize_timestamp(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %I:%M %p")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")

    text = str(value).strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt).strftime("%d %b %Y, %I:%M %p")
        except ValueError:
            continue
    return text


def _send_and_log(message_text: str, resend_of_message_id: int | None = None) -> tuple[int | None, str | None]:
    text = validate_message_text(message_text)
    result = send_telegram_text(text)

    status = "SENT" if result.get("ok") else "FAILED"
    message_id = create_message_record(
        message_text=text,
        status=status,
        telegram_message_id=result.get("telegram_message_id"),
        telegram_chat_id=result.get("telegram_chat_id"),
        http_status=result.get("http_status"),
        response_payload=result.get("response_payload"),
        error_message=result.get("error_message"),
        resend_of_message_id=resend_of_message_id,
    )
    return message_id, (None if status == "SENT" else result.get("error_message") or "Send failed")


def _require_valid_api_key():
    raw_key = extract_api_key_from_request(request)
    if not raw_key:
        return None, (
            jsonify(
                {
                    "ok": False,
                    "error": "API key missing",
                    "header_name": get_api_key_header_name(),
                }
            ),
            400,
        )

    matched = find_active_api_key_match(raw_key)
    if not matched:
        return None, (jsonify({"ok": False, "error": "Invalid API key"}), 401)

    return matched, None


def _process_inbound_message(message_id: int, text: str, forced_type: str | None = None) -> str:
    message_type = (forced_type or _classify_message(text) or "UNCATEGORIZED").strip().upper()
    if message_type not in {"BOOKMARK", "TRANSACTION", "UNCATEGORIZED"}:
        raise ValueError("Invalid message type")

    if message_type == "BOOKMARK":
        url, title = _extract_bookmark_payload(text)
        if not url:
            raise ValueError("URL not found for bookmark")
        existing = get_bookmark_by_url(url)
        if existing:
            bookmark_id = int(existing["bookmark_id"])
        else:
            bookmark_id = create_bookmark(
                url=url,
                title=title[:500],
                category=None,
                starred=0,
                notes=f"Imported from Telegram message #{message_id}",
            )
        mark_inbound_processed(message_id=message_id, message_type="BOOKMARK", bookmark_id=bookmark_id)
        return "BOOKMARK"

    if message_type == "TRANSACTION":
        transaction_id = _create_transaction_from_message(text)
        mark_inbound_processed(message_id=message_id, message_type="TRANSACTION", transaction_id=transaction_id)
        return "TRANSACTION"

    mark_inbound_processed(message_id=message_id, message_type="UNCATEGORIZED")
    return "UNCATEGORIZED"


def _analyze_new_inbound_messages(limit: int = 500) -> tuple[int, int, int, int]:
    items = list_new_inbound_messages(limit=limit)
    bookmark_count = 0
    tx_count = 0
    uncategorized_count = 0
    failed_count = 0

    for item in items:
        message_id = int(item["message_id"])
        text = (item.get("message_text") or "").strip()
        try:
            message_type = _process_inbound_message(message_id=message_id, text=text)
            if message_type == "BOOKMARK":
                bookmark_count += 1
            elif message_type == "TRANSACTION":
                tx_count += 1
            else:
                uncategorized_count += 1
        except Exception as exc:
            mark_inbound_failed(message_id=message_id, error_message=str(exc))
            failed_count += 1

    return bookmark_count, tx_count, uncategorized_count, failed_count


@messenger_bp.get("/messenger")
def messenger_list():
    search = request.args.get("q", "")
    status = request.args.get("status", "all")
    show_hidden = (request.args.get("show_hidden") or "").strip().lower() in {"1", "true", "y", "yes"}
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages, status, show_hidden = list_messages(
        search=search,
        status=status,
        show_hidden=show_hidden,
        page=page,
        page_size=page_size,
    )
    for item in items:
        item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
        item["processed_at_human"] = _humanize_timestamp(item.get("processed_at"))
        text = (item.get("message_text") or "").strip()
        item["message_text_preview"] = text if len(text) <= 140 else f"{text[:140]}..."

    return render_template(
        "messenger/list.html",
        items=items,
        search=search,
        status=status.lower(),
        show_hidden=show_hidden,
        page=page,
        total_pages=total_pages,
        message=request.args.get("msg"),
    )


@messenger_bp.get("/messenger/new")
def messenger_new_page():
    return render_template(
        "messenger/form.html",
        item=None,
        error_message=None,
    )


@messenger_bp.post("/messenger/new")
def messenger_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template(
            "messenger/form.html",
            item=None,
            error_message="Session expired. Please try again.",
        ), 400

    message_text = request.form.get("message_text") or ""
    item = {"message_text": message_text}
    try:
        message_id, send_error = _send_and_log(message_text)
        if message_id is None:
            return render_template("messenger/form.html", item=item, error_message="Failed to save message"), 500
        if send_error:
            return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="failed"))
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="sent"))
    except ValueError as exc:
        return render_template("messenger/form.html", item=item, error_message=str(exc)), 400


@messenger_bp.get("/messenger/<int:message_id>")
def messenger_detail(message_id: int):
    item = get_message(message_id)
    if not item:
        return render_template("shared/error.html"), 404

    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    parent = None
    if item.get("resend_of_message_id"):
        parent = get_message(int(item["resend_of_message_id"]))

    return render_template(
        "messenger/detail.html",
        item=item,
        parent=parent,
        message=request.args.get("msg"),
        humanize_timestamp=_humanize_timestamp,
        can_resend=item.get("direction") != "INBOUND",
    )


@messenger_bp.post("/messenger/<int:message_id>/resend")
def messenger_resend(message_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    original = get_message(message_id)
    if not original:
        return render_template("shared/error.html"), 404

    try:
        new_message_id, send_error = _send_and_log(
            original.get("message_text") or "",
            resend_of_message_id=message_id,
        )
        if new_message_id is None:
            return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="resend_failed"))
        if send_error:
            return redirect(url_for("messenger.messenger_detail", message_id=new_message_id, msg="resend_failed"))
        return redirect(url_for("messenger.messenger_detail", message_id=new_message_id, msg="resent"))
    except ValueError as exc:
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg=str(exc)))


@messenger_bp.post("/messenger/<int:message_id>/hide")
def messenger_hide(message_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    hide_message(message_id)
    return redirect(url_for("messenger.messenger_list", msg="hidden"))


@messenger_bp.post("/messenger/hide-all")
def messenger_hide_all():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    count = hide_all_messages()
    return redirect(url_for("messenger.messenger_list", msg=f"hidden_all_{count}"))


@messenger_bp.post("/messenger/<int:message_id>/unhide")
def messenger_unhide(message_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    unhide_message(message_id)
    return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="unhidden"))


@messenger_bp.post("/messenger/fetch-incoming")
def messenger_fetch_incoming():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    try:
        chat_id = get_telegram_default_chat_id()
        last_update_id = get_last_inbound_update_id(chat_id=chat_id)
        result = fetch_telegram_updates(chat_id=chat_id, offset=(last_update_id + 1) if last_update_id > 0 else None, limit=100)
        if not result.get("ok"):
            return redirect(url_for("messenger.messenger_list", msg=f"fetch_failed: {result.get('error_message') or 'unknown'}"))

        created = 0
        for update in result.get("updates") or []:
            message_id = create_inbound_message_record(
                message_text=update.get("message_text") or "",
                telegram_update_id=int(update.get("update_id")),
                telegram_message_id=update.get("telegram_message_id"),
                telegram_chat_id=update.get("telegram_chat_id"),
                response_payload=None,
            )
            if message_id is not None:
                created += 1

        return redirect(url_for("messenger.messenger_list", msg=f"fetched_{created}"))
    except Exception as exc:
        return redirect(url_for("messenger.messenger_list", msg=f"fetch_failed: {str(exc)}"))


@messenger_bp.post("/messenger/fetch-and-analyze")
def messenger_fetch_and_analyze_incoming():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    try:
        chat_id = get_telegram_default_chat_id()
        last_update_id = get_last_inbound_update_id(chat_id=chat_id)
        result = fetch_telegram_updates(chat_id=chat_id, offset=(last_update_id + 1) if last_update_id > 0 else None, limit=100)
        if not result.get("ok"):
            return redirect(url_for("messenger.messenger_list", msg=f"fetch_failed: {result.get('error_message') or 'unknown'}"))

        created = 0
        for update in result.get("updates") or []:
            message_id = create_inbound_message_record(
                message_text=update.get("message_text") or "",
                telegram_update_id=int(update.get("update_id")),
                telegram_message_id=update.get("telegram_message_id"),
                telegram_chat_id=update.get("telegram_chat_id"),
                response_payload=None,
            )
            if message_id is not None:
                created += 1

        bookmark_count, tx_count, uncategorized_count, failed_count = _analyze_new_inbound_messages(limit=500)
        summary = f"fetch_analyze_f{created}_b{bookmark_count}_t{tx_count}_u{uncategorized_count}_f{failed_count}"
        return redirect(url_for("messenger.messenger_list", msg=summary))
    except Exception as exc:
        return redirect(url_for("messenger.messenger_list", msg=f"fetch_failed: {str(exc)}"))


@messenger_bp.post("/messenger/analyze-incoming")
def messenger_analyze_incoming():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    bookmark_count, tx_count, uncategorized_count, failed_count = _analyze_new_inbound_messages(limit=500)

    summary = f"analyzed_b{bookmark_count}_t{tx_count}_u{uncategorized_count}_f{failed_count}"
    return redirect(url_for("messenger.messenger_list", msg=summary))


@messenger_bp.post("/messenger/<int:message_id>/assign-category")
def messenger_assign_category(message_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_message(message_id)
    if not item:
        return render_template("shared/error.html"), 404
    if item.get("direction") != "INBOUND":
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="assign_not_allowed"))

    forced_type = (request.form.get("message_type") or "").strip().upper()
    if forced_type not in {"BOOKMARK", "TRANSACTION", "UNCATEGORIZED"}:
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="invalid_manual_type"))

    try:
        _process_inbound_message(message_id=message_id, text=(item.get("message_text") or ""), forced_type=forced_type)
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="assigned"))
    except Exception as exc:
        mark_inbound_failed(message_id=message_id, error_message=str(exc))
        return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="assign_failed"))


@messenger_bp.post("/messenger/api/read-analyze")
def messenger_api_read_analyze():
    _api_key_row, api_error = _require_valid_api_key()
    if api_error:
        return api_error

    try:
        chat_id = get_telegram_default_chat_id()
        last_update_id = get_last_inbound_update_id(chat_id=chat_id)
        result = fetch_telegram_updates(chat_id=chat_id, offset=(last_update_id + 1) if last_update_id > 0 else None, limit=100)
        if not result.get("ok"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "fetch_failed",
                        "message": result.get("error_message") or "unknown",
                    }
                ),
                502,
            )

        fetched_count = 0
        for update in result.get("updates") or []:
            message_id = create_inbound_message_record(
                message_text=update.get("message_text") or "",
                telegram_update_id=int(update.get("update_id")),
                telegram_message_id=update.get("telegram_message_id"),
                telegram_chat_id=update.get("telegram_chat_id"),
                response_payload=None,
            )
            if message_id is not None:
                fetched_count += 1

        bookmark_count, tx_count, uncategorized_count, failed_count = _analyze_new_inbound_messages(limit=500)

        return jsonify(
            {
                "ok": True,
                "fetched_count": fetched_count,
                "analyzed": {
                    "bookmark_count": bookmark_count,
                    "transaction_count": tx_count,
                    "uncategorized_count": uncategorized_count,
                    "failed_count": failed_count,
                },
                "summary": {
                    "fetched": fetched_count,
                    "bookmarks": bookmark_count,
                    "transactions": tx_count,
                    "uncategorized": uncategorized_count,
                    "failed": failed_count,
                },
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": "internal_error", "message": str(exc)}), 500

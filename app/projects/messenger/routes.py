from datetime import date, datetime

from flask import Blueprint, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from app.core.messenger import send_telegram_text
from .repository import (
    create_message_record,
    get_message,
    hide_message,
    list_messages,
    unhide_message,
    validate_message_text,
)

messenger_bp = Blueprint("messenger", __name__)


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


@messenger_bp.post("/messenger/<int:message_id>/unhide")
def messenger_unhide(message_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    unhide_message(message_id)
    return redirect(url_for("messenger.messenger_detail", message_id=message_id, msg="unhidden"))

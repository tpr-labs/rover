from datetime import date, datetime

from flask import Blueprint, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    create_shortcut,
    deactivate_shortcut,
    delete_shortcut,
    get_shortcut,
    list_shortcuts,
    restore_shortcut,
    update_shortcut,
)

shortcuts_bp = Blueprint("shortcuts", __name__)


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


@shortcuts_bp.get("/shortcuts")
def shortcuts_list():
    search = request.args.get("q", "")
    status = request.args.get("status", "active")
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages, status = list_shortcuts(search=search, status=status, page=page, page_size=page_size)
    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    return render_template(
        "shortcuts/list.html",
        items=items,
        search=search,
        status=status,
        page=page,
        total_pages=total_pages,
    )


@shortcuts_bp.get("/shortcuts/new")
def shortcuts_new_page():
    return render_template("shortcuts/form.html", mode="create", item=None, error_message=None)


@shortcuts_bp.post("/shortcuts/new")
def shortcuts_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shortcuts/form.html", mode="create", item=None, error_message="Session expired. Please try again."), 400

    item = {
        "item_key": (request.form.get("item_key") or "").strip(),
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
    }
    try:
        create_shortcut(item["item_key"], item["item_value"], item["additional_info"])
        return redirect(url_for("shortcuts.shortcuts_detail", item_key=item["item_key"], msg="created"))
    except ValueError as exc:
        return render_template("shortcuts/form.html", mode="create", item=item, error_message=str(exc)), 400


@shortcuts_bp.get("/shortcuts/<path:item_key>")
def shortcuts_detail(item_key: str):
    item = get_shortcut(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("shortcuts/detail.html", item=item, message=request.args.get("msg"))


@shortcuts_bp.get("/shortcuts/<path:item_key>/edit")
def shortcuts_edit_page(item_key: str):
    item = get_shortcut(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("shortcuts/form.html", mode="edit", item=item, error_message=None)


@shortcuts_bp.post("/shortcuts/<path:item_key>/edit")
def shortcuts_edit_submit(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "item_key": item_key,
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
        "is_active": request.form.get("is_active") or "Y",
    }
    try:
        ok = update_shortcut(item_key, item["item_value"], item["additional_info"], item["is_active"])
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("shortcuts.shortcuts_detail", item_key=item_key, msg="updated"))
    except ValueError as exc:
        return render_template("shortcuts/form.html", mode="edit", item=item, error_message=str(exc)), 400


@shortcuts_bp.post("/shortcuts/<path:item_key>/delete")
def shortcuts_delete(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    deactivate_shortcut(item_key)
    return redirect(url_for("shortcuts.shortcuts_list", msg="deactivated"))


@shortcuts_bp.post("/shortcuts/<path:item_key>/restore")
def shortcuts_restore(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_shortcut(item_key)
    return redirect(url_for("shortcuts.shortcuts_detail", item_key=item_key, msg="restored"))


@shortcuts_bp.post("/shortcuts/<path:item_key>/delete-permanent")
def shortcuts_delete_permanent(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_shortcut(item_key)
    return redirect(url_for("shortcuts.shortcuts_list", msg="deleted"))

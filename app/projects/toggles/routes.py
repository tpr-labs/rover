from datetime import date, datetime

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    create_toggle,
    deactivate_toggle,
    delete_toggle,
    get_toggle,
    list_toggles,
    restore_toggle,
    switch_toggle,
    update_toggle,
)

toggles_bp = Blueprint("toggles", __name__)


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


@toggles_bp.get("/toggles")
def toggles_list():
    search = request.args.get("q", "")
    status = request.args.get("status", "active")
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages, status = list_toggles(search=search, status=status, page=page, page_size=page_size)
    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    return render_template(
        "toggles/list.html",
        items=items,
        search=search,
        status=status,
        page=page,
        total_pages=total_pages,
    )


@toggles_bp.get("/toggles/new")
def toggles_new_page():
    return render_template("toggles/form.html", mode="create", item=None, error_message=None)


@toggles_bp.post("/toggles/new")
def toggles_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("toggles/form.html", mode="create", item=None, error_message="Session expired. Please try again."), 400

    item = {
        "item_key": (request.form.get("item_key") or "").strip(),
        "item_value": request.form.get("item_value") or "N",
        "additional_info": request.form.get("additional_info") or "",
    }
    try:
        create_toggle(item["item_key"], item["additional_info"], item["item_value"])
        return redirect(url_for("toggles.toggles_detail", item_key=item["item_key"], msg="created"))
    except ValueError as exc:
        return render_template("toggles/form.html", mode="create", item=item, error_message=str(exc)), 400


@toggles_bp.get("/toggles/<path:item_key>")
def toggles_detail(item_key: str):
    item = get_toggle(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("toggles/detail.html", item=item, message=request.args.get("msg"))


@toggles_bp.get("/toggles/<path:item_key>/edit")
def toggles_edit_page(item_key: str):
    item = get_toggle(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("toggles/form.html", mode="edit", item=item, error_message=None)


@toggles_bp.post("/toggles/<path:item_key>/edit")
def toggles_edit_submit(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "item_key": item_key,
        "item_value": request.form.get("item_value") or "N",
        "additional_info": request.form.get("additional_info") or "",
        "is_active": request.form.get("is_active") or "Y",
    }
    try:
        ok = update_toggle(item_key, item["additional_info"], item["item_value"], item["is_active"])
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("toggles.toggles_detail", item_key=item_key, msg="updated"))
    except ValueError as exc:
        return render_template("toggles/form.html", mode="edit", item=item, error_message=str(exc)), 400


@toggles_bp.post("/toggles/<path:item_key>/switch")
def toggles_switch(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "Session expired"}), 400

    desired = (request.form.get("value") or "").strip().upper()
    try:
        ok = switch_toggle(item_key, desired)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if not ok:
        return jsonify({"ok": False, "error": "Toggle not found or inactive"}), 404
    return jsonify({"ok": True, "value": desired})


@toggles_bp.post("/toggles/<path:item_key>/delete")
def toggles_delete(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    deactivate_toggle(item_key)
    return redirect(url_for("toggles.toggles_list", msg="deactivated"))


@toggles_bp.post("/toggles/<path:item_key>/restore")
def toggles_restore(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_toggle(item_key)
    return redirect(url_for("toggles.toggles_detail", item_key=item_key, msg="restored"))


@toggles_bp.post("/toggles/<path:item_key>/delete-permanent")
def toggles_delete_permanent(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_toggle(item_key)
    return redirect(url_for("toggles.toggles_list", msg="deleted"))

import json
from datetime import date, datetime

from flask import Blueprint, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    create_dashboard_item,
    create_shortcut,
    deactivate_dashboard_item,
    deactivate_shortcut,
    delete_dashboard_item,
    delete_shortcut,
    get_dashboard_item,
    get_shortcut,
    list_dashboard_items,
    list_shortcuts,
    restore_dashboard_item,
    restore_shortcut,
    update_dashboard_item,
    update_shortcut,
)

shortcuts_bp = Blueprint("shortcuts", __name__)


def _parse_metadata(raw: str | None) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError("Additional info must be valid JSON")
    if not isinstance(payload, dict):
        raise ValueError("Additional info must be a JSON object")
    return payload


def _build_dashboard_metadata(raw_json: str | None, icon_class: str | None, display_order: str | None) -> str:
    payload = _parse_metadata(raw_json)

    icon = (icon_class or "").strip()
    if icon:
        payload["icon"] = icon
    else:
        payload.pop("icon", None)

    order_text = (display_order or "").strip()
    if order_text:
        try:
            order_int = int(order_text)
        except ValueError:
            raise ValueError("Display order must be a positive integer")
        if order_int < 1:
            raise ValueError("Display order must be at least 1")
        payload["order"] = order_int
    else:
        payload.pop("order", None)

    return json.dumps(payload) if payload else ""


def _extract_dashboard_form_fields(item: dict | None) -> tuple[str, str]:
    if not item:
        return "", ""
    raw = (item.get("additional_info") or "").strip()
    if not raw:
        return "", ""
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return "", ""
    if not isinstance(payload, dict):
        return "", ""

    icon_class = str(payload.get("icon") or "").strip()
    order_value = payload.get("order")
    if isinstance(order_value, bool):
        order_text = ""
    elif order_value is None:
        order_text = ""
    else:
        order_text = str(order_value).strip()
    return icon_class, order_text


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
    dashboard_page = max(1, int(request.args.get("dpage", "1")))
    page_size = 20

    items, total_pages, status = list_shortcuts(search=search, status=status, page=page, page_size=page_size)
    dashboard_items, dashboard_total_pages, _ = list_dashboard_items(
        search=search,
        status=status,
        page=dashboard_page,
        page_size=page_size,
    )

    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    for item in dashboard_items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))

    return render_template(
        "shortcuts/list.html",
        items=items,
        dashboard_items=dashboard_items,
        search=search,
        status=status,
        page=page,
        total_pages=total_pages,
        dashboard_page=dashboard_page,
        dashboard_total_pages=dashboard_total_pages,
    )


@shortcuts_bp.get("/shortcuts/dashboard-items")
def dashboard_items_list():
    return redirect(
        url_for(
            "shortcuts.shortcuts_list",
            q=request.args.get("q", ""),
            status=request.args.get("status", "active"),
            dpage=request.args.get("page", "1"),
        )
    )


@shortcuts_bp.get("/shortcuts/dashboard-items/new")
def dashboard_items_new_page():
    return render_template(
        "shortcuts/dashboard_form.html",
        mode="create",
        item=None,
        error_message=None,
        icon_class="",
        display_order="",
    )


@shortcuts_bp.post("/shortcuts/dashboard-items/new")
def dashboard_items_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shortcuts/dashboard_form.html", mode="create", item=None, error_message="Session expired. Please try again."), 400

    item = {
        "item_key": (request.form.get("item_key") or "").strip(),
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
        "icon_class": request.form.get("icon_class") or "",
        "display_order": request.form.get("display_order") or "",
    }
    try:
        metadata = _build_dashboard_metadata(item["additional_info"], item["icon_class"], item["display_order"])
        create_dashboard_item(item["item_key"], item["item_value"], metadata)
        return redirect(url_for("shortcuts.dashboard_items_detail", item_key=item["item_key"], msg="created"))
    except ValueError as exc:
        return render_template(
            "shortcuts/dashboard_form.html",
            mode="create",
            item=item,
            error_message=str(exc),
            icon_class=item["icon_class"],
            display_order=item["display_order"],
        ), 400


@shortcuts_bp.get("/shortcuts/dashboard-items/<path:item_key>")
def dashboard_items_detail(item_key: str):
    item = get_dashboard_item(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("shortcuts/dashboard_detail.html", item=item, message=request.args.get("msg"))


@shortcuts_bp.get("/shortcuts/dashboard-items/<path:item_key>/edit")
def dashboard_items_edit_page(item_key: str):
    item = get_dashboard_item(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    icon_class, display_order = _extract_dashboard_form_fields(item)
    return render_template(
        "shortcuts/dashboard_form.html",
        mode="edit",
        item=item,
        error_message=None,
        icon_class=icon_class,
        display_order=display_order,
    )


@shortcuts_bp.post("/shortcuts/dashboard-items/<path:item_key>/edit")
def dashboard_items_edit_submit(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "item_key": item_key,
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
        "icon_class": request.form.get("icon_class") or "",
        "display_order": request.form.get("display_order") or "",
        "is_active": request.form.get("is_active") or "Y",
    }
    try:
        metadata = _build_dashboard_metadata(item["additional_info"], item["icon_class"], item["display_order"])
        ok = update_dashboard_item(item_key, item["item_value"], metadata, item["is_active"])
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("shortcuts.dashboard_items_detail", item_key=item_key, msg="updated"))
    except ValueError as exc:
        return render_template(
            "shortcuts/dashboard_form.html",
            mode="edit",
            item=item,
            error_message=str(exc),
            icon_class=item["icon_class"],
            display_order=item["display_order"],
        ), 400


@shortcuts_bp.post("/shortcuts/dashboard-items/<path:item_key>/delete")
def dashboard_items_delete(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    deactivate_dashboard_item(item_key)
    return redirect(url_for("shortcuts.dashboard_items_list", msg="deactivated"))


@shortcuts_bp.post("/shortcuts/dashboard-items/<path:item_key>/restore")
def dashboard_items_restore(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_dashboard_item(item_key)
    return redirect(url_for("shortcuts.dashboard_items_detail", item_key=item_key, msg="restored"))


@shortcuts_bp.post("/shortcuts/dashboard-items/<path:item_key>/delete-permanent")
def dashboard_items_delete_permanent(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_dashboard_item(item_key)
    return redirect(url_for("shortcuts.dashboard_items_list", msg="deleted"))


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

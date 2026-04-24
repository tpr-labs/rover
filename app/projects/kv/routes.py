from flask import Blueprint, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import create_item, deactivate_item, get_item, list_items, restore_item, update_item

kv_bp = Blueprint("kv", __name__)


@kv_bp.get("/kv")
def kv_list():
    search = request.args.get("q", "")
    category = request.args.get("category", "")
    status = request.args.get("status", "active")
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages, status = list_items(search=search, category=category, status=status, page=page, page_size=page_size)
    return render_template(
        "kv/list.html",
        items=items,
        search=search,
        category=category,
        status=status,
        page=page,
        total_pages=total_pages,
    )


@kv_bp.get("/kv/new")
def kv_new_page():
    return render_template("kv/form.html", mode="create", item=None, error_message=None)


@kv_bp.post("/kv/new")
def kv_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("kv/form.html", mode="create", item=None, error_message="Session expired. Please try again."), 400

    item = {
        "item_key": (request.form.get("item_key") or "").strip(),
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
        "category": request.form.get("category") or "",
    }
    try:
        create_item(item["item_key"], item["item_value"], item["additional_info"], item["category"])
        return redirect(url_for("kv.kv_detail", item_key=item["item_key"], msg="created"))
    except ValueError as exc:
        return render_template("kv/form.html", mode="create", item=item, error_message=str(exc)), 400


@kv_bp.get("/kv/<path:item_key>")
def kv_detail(item_key: str):
    item = get_item(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("kv/detail.html", item=item, message=request.args.get("msg"))


@kv_bp.get("/kv/<path:item_key>/edit")
def kv_edit_page(item_key: str):
    item = get_item(item_key)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("kv/form.html", mode="edit", item=item, error_message=None)


@kv_bp.post("/kv/<path:item_key>/edit")
def kv_edit_submit(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "item_key": item_key,
        "item_value": request.form.get("item_value") or "",
        "additional_info": request.form.get("additional_info") or "",
        "category": request.form.get("category") or "",
        "is_active": request.form.get("is_active") or "Y",
    }
    try:
        ok = update_item(item_key, item["item_value"], item["additional_info"], item["category"], item["is_active"])
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("kv.kv_detail", item_key=item_key, msg="updated"))
    except ValueError as exc:
        return render_template("kv/form.html", mode="edit", item=item, error_message=str(exc)), 400


@kv_bp.post("/kv/<path:item_key>/delete")
def kv_delete(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    deactivate_item(item_key)
    return redirect(url_for("kv.kv_list", msg="deactivated"))


@kv_bp.post("/kv/<path:item_key>/restore")
def kv_restore(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_item(item_key)
    return redirect(url_for("kv.kv_detail", item_key=item_key, msg="restored"))

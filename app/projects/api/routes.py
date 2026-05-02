from datetime import date, datetime

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.api_keys import build_key_reference, generate_api_key, hash_api_key, key_preview_parts
from app.core.auth import is_valid_csrf
from .repository import (
    activate_api_key,
    create_api_key_record,
    deactivate_api_key,
    delete_api_key_permanent,
    extract_api_key_from_request,
    find_active_api_key_match,
    get_api_key,
    get_api_key_header_name,
    list_api_keys,
    validate_api_key_input,
)

api_bp = Blueprint("api_project", __name__)


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


def _create_key_with_retry(*, name: str, notes: str | None, rotated_from: str | None = None) -> tuple[dict, str]:
    raw_key = generate_api_key()
    key_hash = hash_api_key(raw_key)
    prefix, last4 = key_preview_parts(raw_key)

    last_exc: ValueError | None = None
    for _ in range(5):
        ref = build_key_reference()
        try:
            create_api_key_record(
                item_key=ref,
                key_hash=key_hash,
                name=name,
                prefix=prefix,
                last4=last4,
                notes=notes,
                rotated_from=rotated_from,
            )
            row = get_api_key(ref)
            if not row:
                raise ValueError("Failed to read created API key")
            return row, raw_key
        except ValueError as exc:
            if "reference already exists" not in str(exc):
                raise
            last_exc = exc
    if last_exc:
        raise last_exc
    raise ValueError("Failed to create API key")


@api_bp.get("/api")
def api_keys_list():
    search = request.args.get("q", "")
    status = request.args.get("status", "active")
    page = max(1, int(request.args.get("page", "1")))

    items, total_pages, status = list_api_keys(search=search, status=status, page=page, page_size=20)
    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))

    return render_template(
        "api/list.html",
        items=items,
        search=search,
        status=status,
        page=page,
        total_pages=total_pages,
        message=request.args.get("msg"),
    )


@api_bp.get("/api/new")
def api_keys_new_page():
    return render_template("api/form.html", item=None, error_message=None)


@api_bp.post("/api/new")
def api_keys_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("api/form.html", item=None, error_message="Session expired. Please try again."), 400

    name = request.form.get("name") or ""
    notes = request.form.get("notes") or ""
    item = {"name": name, "notes": notes}

    try:
        clean_name = validate_api_key_input(name, notes)
        row, raw_key = _create_key_with_retry(name=clean_name, notes=notes)
        row["created_at_human"] = _humanize_timestamp(row.get("created_at"))
        row["updated_at_human"] = _humanize_timestamp(row.get("updated_at"))
        return render_template("api/detail.html", item=row, created_key=raw_key, message="created")
    except ValueError as exc:
        return render_template("api/form.html", item=item, error_message=str(exc)), 400


@api_bp.get("/api/<path:item_key>")
def api_keys_detail(item_key: str):
    item = get_api_key(item_key)
    if not item:
        return render_template("shared/error.html"), 404

    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    return render_template("api/detail.html", item=item, created_key=None, message=request.args.get("msg"))


@api_bp.post("/api/<path:item_key>/activate")
def api_keys_activate(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    activate_api_key(item_key)
    return redirect(url_for("api_project.api_keys_detail", item_key=item_key, msg="activated"))


@api_bp.post("/api/<path:item_key>/deactivate")
def api_keys_deactivate(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    deactivate_api_key(item_key)
    return redirect(url_for("api_project.api_keys_detail", item_key=item_key, msg="deactivated"))


@api_bp.post("/api/<path:item_key>/delete-permanent")
def api_keys_delete_permanent(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_api_key_permanent(item_key)
    return redirect(url_for("api_project.api_keys_list", msg="deleted"))


@api_bp.post("/api/<path:item_key>/rotate")
def api_keys_rotate(item_key: str):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    old = get_api_key(item_key)
    if not old:
        return render_template("shared/error.html"), 404

    new_name = (request.form.get("new_name") or "").strip() or old.get("name") or item_key
    notes = request.form.get("notes") or old.get("notes") or ""

    try:
        clean_name = validate_api_key_input(new_name, notes)
        new_row, raw_key = _create_key_with_retry(name=clean_name, notes=notes, rotated_from=item_key)
        deactivate_api_key(item_key)
        new_row["created_at_human"] = _humanize_timestamp(new_row.get("created_at"))
        new_row["updated_at_human"] = _humanize_timestamp(new_row.get("updated_at"))
        return render_template("api/detail.html", item=new_row, created_key=raw_key, message="rotated")
    except ValueError as exc:
        return redirect(url_for("api_project.api_keys_detail", item_key=item_key, msg=str(exc)))


@api_bp.get("/api/validate-key")
def api_validate_key():
    raw_key = extract_api_key_from_request(request)
    if not raw_key:
        return jsonify(
            {
                "valid": False,
                "error": "API key missing",
                "header_name": get_api_key_header_name(),
            }
        ), 400

    matched = find_active_api_key_match(raw_key)
    if not matched:
        return jsonify({"valid": False, "error": "Invalid API key"}), 401

    return jsonify(
        {
            "valid": True,
            "item_key": matched.get("item_key"),
            "name": matched.get("name"),
            "prefix": matched.get("prefix"),
            "last4": matched.get("last4"),
        }
    )

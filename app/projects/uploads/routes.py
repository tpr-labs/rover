from datetime import date, datetime

from flask import Blueprint, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    add_upload_link,
    create_upload_record,
    delete_upload_record,
    get_upload,
    get_upload_par_url,
    is_upload_allowed,
    list_link_candidates,
    list_upload_links,
    list_uploads,
    remove_upload_link,
    update_upload_record,
    upload_file_to_oci,
)

uploads_bp = Blueprint("uploads", __name__)


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


def _upload_status() -> tuple[bool, str]:
    if not is_upload_allowed():
        return False, "Upload is disabled by toggle ALLOW_OCI_FILE_UPLOAD"
    try:
        get_upload_par_url()
    except ValueError as exc:
        return False, str(exc)
    return True, "Upload is enabled"


@uploads_bp.get("/uploads")
def uploads_list():
    search = request.args.get("q", "")
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages = list_uploads(search=search, page=page, page_size=page_size)
    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))

    upload_ready, upload_status_message = _upload_status()
    return render_template(
        "uploads/list.html",
        items=items,
        search=search,
        page=page,
        total_pages=total_pages,
        upload_ready=upload_ready,
        upload_status_message=upload_status_message,
    )


@uploads_bp.get("/uploads/new")
def uploads_new_page():
    upload_ready, upload_status_message = _upload_status()
    return render_template(
        "uploads/form.html",
        mode="create",
        item=None,
        error_message=None,
        upload_ready=upload_ready,
        upload_status_message=upload_status_message,
    )


@uploads_bp.post("/uploads/new")
def uploads_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("uploads/form.html", mode="create", item=None, error_message="Session expired. Please try again.", upload_ready=False, upload_status_message="Session error"), 400

    title = (request.form.get("title") or "").strip()
    notes = request.form.get("notes") or ""
    uploaded_file = request.files.get("file")

    if not uploaded_file or not (uploaded_file.filename or "").strip():
        upload_ready, upload_status_message = _upload_status()
        item = {"title": title, "notes": notes}
        return render_template(
            "uploads/form.html",
            mode="create",
            item=item,
            error_message="File is required",
            upload_ready=upload_ready,
            upload_status_message=upload_status_message,
        ), 400

    original_file_name = uploaded_file.filename.strip()
    file_bytes = uploaded_file.read()
    content_type = uploaded_file.mimetype or "application/octet-stream"
    if not title:
        title = original_file_name

    try:
        object_name, object_url, size_bytes, normalized_content_type = upload_file_to_oci(
            original_file_name=original_file_name,
            file_bytes=file_bytes,
            content_type=content_type,
        )
        upload_id = create_upload_record(
            title=title,
            original_file_name=original_file_name,
            content_type=normalized_content_type,
            size_bytes=size_bytes,
            object_name=object_name,
            object_url=object_url,
            notes=notes,
        )
        return redirect(url_for("uploads.uploads_detail", upload_id=upload_id, msg="created"))
    except ValueError as exc:
        upload_ready, upload_status_message = _upload_status()
        item = {"title": title, "notes": notes}
        return render_template(
            "uploads/form.html",
            mode="create",
            item=item,
            error_message=str(exc),
            upload_ready=upload_ready,
            upload_status_message=upload_status_message,
        ), 400


@uploads_bp.get("/uploads/<int:upload_id>")
def uploads_detail(upload_id: int):
    item = get_upload(upload_id)
    if not item:
        return render_template("shared/error.html"), 404

    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    links = list_upload_links(upload_id)
    candidates = list_link_candidates(upload_id)
    return render_template(
        "uploads/detail.html",
        item=item,
        links=links,
        link_candidates=candidates,
        message=request.args.get("msg"),
    )


@uploads_bp.get("/uploads/<int:upload_id>/edit")
def uploads_edit_page(upload_id: int):
    item = get_upload(upload_id)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("uploads/form.html", mode="edit", item=item, error_message=None, upload_ready=True, upload_status_message="")


@uploads_bp.post("/uploads/<int:upload_id>/edit")
def uploads_edit_submit(upload_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    title = (request.form.get("title") or "").strip()
    notes = request.form.get("notes") or ""
    item = get_upload(upload_id)
    if not item:
        return render_template("shared/error.html"), 404

    try:
        ok = update_upload_record(upload_id, title, notes)
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("uploads.uploads_detail", upload_id=upload_id, msg="updated"))
    except ValueError as exc:
        item.update({"title": title, "notes": notes})
        return render_template(
            "uploads/form.html",
            mode="edit",
            item=item,
            error_message=str(exc),
            upload_ready=True,
            upload_status_message="",
        ), 400


@uploads_bp.post("/uploads/<int:upload_id>/delete")
def uploads_delete(upload_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_upload(upload_id)
    if not item:
        return render_template("shared/error.html"), 404

    delete_upload_record(upload_id)
    return redirect(url_for("uploads.uploads_list", msg="deleted"))


@uploads_bp.post("/uploads/<int:upload_id>/link")
def uploads_add_link(upload_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    file_id_raw = (request.form.get("file_id") or "").strip()
    if not file_id_raw.isdigit():
        return redirect(url_for("uploads.uploads_detail", upload_id=upload_id))

    try:
        add_upload_link(upload_id, int(file_id_raw))
    except Exception:
        pass
    return redirect(url_for("uploads.uploads_detail", upload_id=upload_id))


@uploads_bp.post("/uploads/<int:upload_id>/unlink/<int:file_id>")
def uploads_remove_link(upload_id: int, file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    remove_upload_link(upload_id, file_id)
    return redirect(url_for("uploads.uploads_detail", upload_id=upload_id))

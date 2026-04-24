import bleach
import markdown
from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    add_file_link,
    autosave_file,
    create_file,
    create_folder,
    get_file,
    get_folder,
    list_active_folders,
    list_file_links,
    list_files,
    list_folders_tree,
    list_link_candidates,
    list_trash,
    purge_file,
    purge_folder,
    remove_file_link,
    restore_file,
    restore_folder,
    search_files,
    trash_file,
    trash_folder,
    update_file,
    update_folder,
)

sb_bp = Blueprint("sb", __name__)


def render_markdown_safely(content_md: str) -> str:
    if hasattr(content_md, "read"):
        content_md = content_md.read() or ""
    elif content_md is None:
        content_md = ""
    else:
        content_md = str(content_md)

    raw_html = markdown.markdown(content_md, extensions=["extra", "sane_lists"])
    allowed_tags = set(bleach.sanitizer.ALLOWED_TAGS).union(
        {
            "p",
            "pre",
            "code",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "hr",
            "br",
            "ul",
            "ol",
            "li",
            "blockquote",
            "table",
            "thead",
            "tbody",
            "tr",
            "th",
            "td",
        }
    )
    return bleach.clean(raw_html, tags=allowed_tags, attributes={"a": ["href", "title", "target"], "code": ["class"]}, strip=True)


@sb_bp.get("/sb")
def sb_home():
    folders = list_folders_tree()
    query = (request.args.get("q") or "").strip()
    search_results = search_files(query) if query else []
    folder_id_raw = request.args.get("folder_id")
    selected_folder = None
    files = []
    if folder_id_raw:
        try:
            folder_id = int(folder_id_raw)
            selected_folder = get_folder(folder_id)
            if selected_folder and selected_folder.get("is_trashed") == "N":
                files = list_files(folder_id)
        except ValueError:
            pass

    return render_template(
        "sb/home.html",
        folders=folders,
        selected_folder=selected_folder,
        files=files,
        query=query,
        search_results=search_results,
    )


@sb_bp.get("/sb/folder/new")
def sb_new_folder_page():
    parent_id = request.args.get("parent_id")
    parent_folder_id = int(parent_id) if parent_id and parent_id.isdigit() else None
    return render_template(
        "sb/folder_form.html",
        mode="create",
        folder=None,
        parent_folder_id=parent_folder_id,
        all_folders=list_active_folders(),
        error_message=None,
    )


@sb_bp.post("/sb/folder/new")
def sb_new_folder_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    folder_name = request.form.get("folder_name", "")
    parent_raw = request.form.get("parent_folder_id", "").strip()
    parent_folder_id = int(parent_raw) if parent_raw else None
    try:
        create_folder(folder_name, parent_folder_id)
        return redirect(url_for("sb.sb_home", folder_id=parent_folder_id) if parent_folder_id else url_for("sb.sb_home"))
    except ValueError as exc:
        return render_template(
            "sb/folder_form.html",
            mode="create",
            folder=None,
            parent_folder_id=parent_folder_id,
            all_folders=list_active_folders(),
            error_message=str(exc),
        ), 400


@sb_bp.get("/sb/folder/<int:folder_id>/edit")
def sb_edit_folder_page(folder_id: int):
    folder = get_folder(folder_id)
    if not folder:
        return render_template("shared/error.html"), 404
    return render_template(
        "sb/folder_form.html",
        mode="edit",
        folder=folder,
        parent_folder_id=folder.get("parent_folder_id"),
        all_folders=[f for f in list_active_folders() if f["folder_id"] != folder_id],
        error_message=None,
    )


@sb_bp.post("/sb/folder/<int:folder_id>/edit")
def sb_edit_folder_submit(folder_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    folder_name = request.form.get("folder_name", "")
    parent_raw = request.form.get("parent_folder_id", "").strip()
    parent_folder_id = int(parent_raw) if parent_raw else None
    try:
        ok = update_folder(folder_id, folder_name, parent_folder_id)
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("sb.sb_home", folder_id=folder_id))
    except ValueError as exc:
        return render_template(
            "sb/folder_form.html",
            mode="edit",
            folder={"folder_id": folder_id, "folder_name": folder_name, "parent_folder_id": parent_folder_id},
            parent_folder_id=parent_folder_id,
            all_folders=[f for f in list_active_folders() if f["folder_id"] != folder_id],
            error_message=str(exc),
        ), 400


@sb_bp.post("/sb/folder/<int:folder_id>/delete")
def sb_delete_folder(folder_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    try:
        trash_folder(folder_id)
    except ValueError:
        return render_template("shared/error.html"), 400
    return redirect(url_for("sb.sb_home"))


@sb_bp.post("/sb/folder/<int:folder_id>/restore")
def sb_restore_folder(folder_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_folder(folder_id)
    return redirect(url_for("sb.sb_trash"))


@sb_bp.post("/sb/folder/<int:folder_id>/purge")
def sb_purge_folder(folder_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    purge_folder(folder_id)
    return redirect(url_for("sb.sb_trash"))


@sb_bp.get("/sb/file/new")
def sb_new_file_page():
    folder_id_raw = request.args.get("folder_id", "")
    folder_id = int(folder_id_raw) if folder_id_raw.isdigit() else None
    return render_template(
        "sb/file_editor.html",
        mode="create",
        file=None,
        selected_folder_id=folder_id,
        all_folders=list_active_folders(),
        links=[],
        link_candidates=[],
        preview_html="",
        error_message=None,
    )


@sb_bp.post("/sb/file/new")
def sb_new_file_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    folder_id = int(request.form.get("folder_id"))
    title = request.form.get("title", "")
    content_md = request.form.get("content_md", "")
    tags = request.form.get("tags", "")
    try:
        file_id = create_file(folder_id, title, content_md, tags)
        return redirect(url_for("sb.sb_file_edit_page", file_id=file_id, msg="created"))
    except ValueError as exc:
        return render_template(
            "sb/file_editor.html",
            mode="create",
            file={"title": title, "content_md": content_md, "tags": tags},
            selected_folder_id=folder_id,
            all_folders=list_active_folders(),
            links=[],
            link_candidates=[],
            preview_html=render_markdown_safely(content_md),
            error_message=str(exc),
        ), 400


@sb_bp.get("/sb/file/<int:file_id>")
def sb_file_view(file_id: int):
    file = get_file(file_id)
    if not file:
        return render_template("shared/error.html"), 404
    html_content = render_markdown_safely(file.get("content_md") or "")
    return render_template("sb/file_view.html", file=file, html_content=html_content, links=list_file_links(file_id))


@sb_bp.get("/sb/file/<int:file_id>/edit")
def sb_file_edit_page(file_id: int):
    file = get_file(file_id)
    if not file:
        return render_template("shared/error.html"), 404
    return render_template(
        "sb/file_editor.html",
        mode="edit",
        file=file,
        selected_folder_id=file.get("folder_id"),
        all_folders=list_active_folders(),
        links=list_file_links(file_id),
        link_candidates=list_link_candidates(file_id),
        preview_html=render_markdown_safely(file.get("content_md") or ""),
        error_message=None,
        message=request.args.get("msg"),
    )


@sb_bp.post("/sb/file/<int:file_id>/edit")
def sb_file_edit_submit(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    folder_id = int(request.form.get("folder_id"))
    title = request.form.get("title", "")
    content_md = request.form.get("content_md", "")
    tags = request.form.get("tags", "")
    try:
        ok = update_file(file_id, title, content_md, tags, folder_id=folder_id)
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("sb.sb_file_edit_page", file_id=file_id, msg="updated"))
    except ValueError as exc:
        file_data = get_file(file_id) or {"file_id": file_id}
        file_data.update({"title": title, "content_md": content_md, "tags": tags, "folder_id": folder_id})
        return render_template(
            "sb/file_editor.html",
            mode="edit",
            file=file_data,
            selected_folder_id=folder_id,
            all_folders=list_active_folders(),
            links=list_file_links(file_id),
            link_candidates=list_link_candidates(file_id),
            preview_html=render_markdown_safely(content_md),
            error_message=str(exc),
        ), 400


@sb_bp.post("/sb/file/<int:file_id>/autosave")
def sb_file_autosave(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "csrf"}), 400
    ok = autosave_file(file_id, request.form.get("content_md", ""))
    return jsonify({"ok": ok})


@sb_bp.post("/sb/file/<int:file_id>/delete")
def sb_file_delete(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    trash_file(file_id)
    return redirect(url_for("sb.sb_home"))


@sb_bp.post("/sb/file/<int:file_id>/restore")
def sb_file_restore(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    restore_file(file_id)
    return redirect(url_for("sb.sb_trash"))


@sb_bp.post("/sb/file/<int:file_id>/purge")
def sb_file_purge(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    purge_file(file_id)
    return redirect(url_for("sb.sb_trash"))


@sb_bp.post("/sb/file/<int:file_id>/link")
def sb_file_add_link(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    target_id = int(request.form.get("target_file_id"))
    try:
        add_file_link(file_id, target_id)
    except ValueError:
        pass
    return redirect(url_for("sb.sb_file_edit_page", file_id=file_id))


@sb_bp.post("/sb/file/<int:file_id>/unlink/<int:target_id>")
def sb_file_remove_link(file_id: int, target_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    remove_file_link(file_id, target_id)
    return redirect(url_for("sb.sb_file_edit_page", file_id=file_id))


@sb_bp.get("/sb/trash")
def sb_trash():
    trash = list_trash()
    return render_template("sb/trash.html", trash=trash)

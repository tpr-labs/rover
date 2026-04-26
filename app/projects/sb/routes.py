import bleach
import importlib
import markdown
import re
from datetime import date, datetime
from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    add_file_link,
    autosave_file,
    create_file,
    create_folder,
    get_file,
    get_public_file_by_token,
    get_graph_snapshot,
    get_folder,
    is_public_content_globally_enabled,
    list_active_folders,
    list_child_folders,
    list_file_links,
    list_files,
    list_link_candidates,
    delete_file,
    delete_folder,
    remove_file_link,
    search_files,
    set_file_public_state,
    update_file,
    update_folder,
)

sb_bp = Blueprint("sb", __name__)


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


def _serialize_timestamp(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def render_markdown_safely(content_md: str) -> str:
    if hasattr(content_md, "read"):
        content_md = content_md.read() or ""
    elif content_md is None:
        content_md = ""
    else:
        content_md = str(content_md)

    extensions = ["extra", "sane_lists", "nl2br"]
    extension_configs = {
        "pymdownx.tasklist": {
            "custom_checkbox": False,
            "clickable_checkbox": False,
        }
    }
    optional_exts = ["pymdownx.tilde", "pymdownx.tasklist", "pymdownx.superfences"]
    for ext in optional_exts:
        module_name = ext.split(".", 1)[0]
        try:
            importlib.import_module(module_name)
            extensions.append(ext)
        except ModuleNotFoundError:
            continue

    raw_html = markdown.markdown(content_md, extensions=extensions, extension_configs=extension_configs)
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
            "del",
            "input",
        }
    )
    return bleach.clean(
        raw_html,
        tags=allowed_tags,
        attributes={
            "a": ["href", "title", "target"],
            "code": ["class"],
            "pre": ["class"],
            "input": ["type", "checked", "disabled"],
            "ul": ["class"],
            "li": ["class"],
        },
        strip=True,
    )


def _build_breadcrumb(folder_id: int | None) -> list[dict]:
    crumbs: list[dict] = [{"folder_id": None, "folder_name": "Root"}]
    if folder_id is None:
        return crumbs

    chain = []
    seen = set()
    current = folder_id
    while current is not None and current not in seen:
        seen.add(current)
        folder = get_folder(current)
        if not folder or folder.get("is_trashed") != "N":
            break
        chain.append({"folder_id": folder["folder_id"], "folder_name": folder["folder_name"]})
        current = folder.get("parent_folder_id")

    crumbs.extend(reversed(chain))
    return crumbs


def _build_file_path(file: dict | None) -> list[dict]:
    if not file:
        return [{"folder_id": None, "folder_name": "Root"}]
    return _build_breadcrumb(file.get("folder_id"))


def _highlight_match(text: str | None, query: str) -> str:
    src = str(text or "")
    q = (query or "").strip()
    if not src or not q:
        return bleach.clean(src)
    pattern = re.compile(re.escape(q), re.IGNORECASE)
    safe_src = bleach.clean(src)
    return pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", safe_src)


def _build_content_snippet(content: str | None, query: str, radius: int = 90) -> str:
    text = " ".join(str(content or "").split())
    q = (query or "").strip().lower()
    if not text:
        return ""
    if not q:
        return text[: radius * 2]

    idx = text.lower().find(q)
    if idx < 0:
        return text[: radius * 2]

    start = max(0, idx - radius)
    end = min(len(text), idx + len(q) + radius)
    snippet = text[start:end]
    if start > 0:
        snippet = f"…{snippet}"
    if end < len(text):
        snippet = f"{snippet}…"
    return snippet


def _search_rank(row: dict, query: str) -> tuple[int, str]:
    q = (query or "").strip().lower()
    result_type = (row.get("result_type") or "file").lower()
    title = str(row.get("title") or "").lower()
    tags = str(row.get("tags") or "").lower()
    content = str(row.get("content_md") or "").lower()

    if result_type == "folder":
        if q and q in title:
            return (0, title)
        return (3, title)

    if q and q in title:
        return (0, title)
    if q and q in tags:
        return (1, tags)
    if q and q in content:
        return (2, content)
    return (3, title)


@sb_bp.get("/sb")
def sb_home():
    query = (request.args.get("q") or "").strip()
    search_results = search_files(query) if query else []
    if query:
        search_results = sorted(search_results, key=lambda row: _search_rank(row, query))
    for row in search_results:
        result_type = (row.get("result_type") or "file").lower()
        title = row.get("title") or ""
        tags = row.get("tags") or ""
        snippet = _build_content_snippet(row.get("content_md") or "", query)
        row["title_highlight"] = _highlight_match(title, query)
        row["tags_highlight"] = _highlight_match(tags, query)
        row["snippet_highlight"] = _highlight_match(snippet, query)
        if result_type == "folder":
            row["result_url"] = url_for("sb.sb_home", folder_id=row.get("folder_id"))
            row["result_kind"] = "Folder"
            row["snippet_highlight"] = ""
        else:
            row["result_url"] = url_for("sb.sb_file_view", file_id=row.get("file_id"))
            row["result_kind"] = "File"

    folder_id_raw = request.args.get("folder_id")
    selected_folder = None
    parent_folder_id = None

    current_folder_id = None
    if folder_id_raw and folder_id_raw.isdigit():
        candidate_id = int(folder_id_raw)
        candidate = get_folder(candidate_id)
        if candidate and candidate.get("is_trashed") == "N":
            selected_folder = candidate
            current_folder_id = candidate_id
            parent_folder_id = candidate.get("parent_folder_id")

    child_folders = list_child_folders(current_folder_id)
    for folder in child_folders:
        folder["updated_at_human"] = _humanize_timestamp(folder.get("updated_at"))

    files = []
    if current_folder_id is not None:
        files = list_files(current_folder_id)
        for file in files:
            file["updated_at_human"] = _humanize_timestamp(file.get("updated_at"))

    breadcrumb = _build_breadcrumb(current_folder_id)

    return render_template(
        "sb/home.html",
        child_folders=child_folders,
        selected_folder=selected_folder,
        current_folder_id=current_folder_id,
        breadcrumb=breadcrumb,
        parent_folder_id=parent_folder_id,
        files=files,
        query=query,
        search_results=search_results,
    )


@sb_bp.get("/sb/graph")
def sb_graph():
    graph_error = None
    graph_data = {"scope": "all", "nodes": [], "links": []}
    try:
        graph_data = get_graph_snapshot(scope="all", root_folder_id=None)
        for node in graph_data.get("nodes", []):
            node["created_at"] = _serialize_timestamp(node.get("created_at"))
            node["updated_at"] = _serialize_timestamp(node.get("updated_at"))
            node["created_at_human"] = _humanize_timestamp(node.get("created_at"))
            node["updated_at_human"] = _humanize_timestamp(node.get("updated_at"))
            if node.get("kind") == "folder":
                node["go_to_url"] = url_for("sb.sb_home", folder_id=node.get("folder_id"))
            elif node.get("kind") == "root_hub":
                node["go_to_url"] = None
            else:
                node["go_to_url"] = url_for("sb.sb_file_view", file_id=node.get("file_id"))
    except Exception:
        graph_error = "Unable to load graph data. Please check DB/config and try again."

    return render_template("sb/graph.html", graph_data=graph_data, graph_error=graph_error)


@sb_bp.post("/sb/markdown/preview")
def sb_markdown_preview():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "Session expired. Please try again."}), 400
    content_md = request.form.get("content_md", "")
    return jsonify({"ok": True, "html": render_markdown_safely(content_md)})


@sb_bp.get("/sb/graph/data")
def sb_graph_data():
    data = get_graph_snapshot(scope="all", root_folder_id=None)
    for node in data.get("nodes", []):
        node["created_at"] = _serialize_timestamp(node.get("created_at"))
        node["updated_at"] = _serialize_timestamp(node.get("updated_at"))
        node["created_at_human"] = _humanize_timestamp(node.get("created_at"))
        node["updated_at_human"] = _humanize_timestamp(node.get("updated_at"))
        if node.get("kind") == "folder":
            node["go_to_url"] = url_for("sb.sb_home", folder_id=node.get("folder_id"))
        elif node.get("kind") == "root_hub":
            node["go_to_url"] = None
        else:
            node["go_to_url"] = url_for("sb.sb_file_view", file_id=node.get("file_id"))

    return jsonify(data)


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
    ok = delete_folder(folder_id)
    if not ok:
        return render_template("shared/error.html"), 404
    return redirect(url_for("sb.sb_home"))


@sb_bp.get("/sb/file/new")
def sb_new_file_page():
    folder_id_raw = request.args.get("folder_id", "")
    folder_id = int(folder_id_raw) if folder_id_raw.isdigit() else None
    return render_template(
        "sb/file_editor.html",
        mode="create",
        file=None,
        file_path=_build_breadcrumb(folder_id),
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
    draft_file_id_raw = (request.form.get("draft_file_id") or "").strip()
    draft_file_id = int(draft_file_id_raw) if draft_file_id_raw.isdigit() else None
    try:
        if draft_file_id is not None:
            ok = update_file(draft_file_id, title, content_md, tags, folder_id=folder_id)
            if not ok:
                return render_template("shared/error.html"), 404
            return redirect(url_for("sb.sb_file_edit_page", file_id=draft_file_id, msg="updated"))

        file_id = create_file(folder_id, title, content_md, tags)
        return redirect(url_for("sb.sb_file_edit_page", file_id=file_id, msg="created"))
    except ValueError as exc:
        return render_template(
            "sb/file_editor.html",
            mode="create",
            file={"title": title, "content_md": content_md, "tags": tags},
            file_path=_build_breadcrumb(folder_id),
            selected_folder_id=folder_id,
            all_folders=list_active_folders(),
            links=[],
            link_candidates=[],
            preview_html=render_markdown_safely(content_md),
            error_message=str(exc),
        ), 400


@sb_bp.post("/sb/file/new/autosave")
def sb_new_file_autosave():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "csrf"}), 400

    folder_raw = (request.form.get("folder_id") or "").strip()
    if not folder_raw.isdigit():
        return jsonify({"ok": False, "error": "folder"}), 400

    folder_id = int(folder_raw)
    title = (request.form.get("title") or "").strip()
    tags = request.form.get("tags", "")
    content_md = request.form.get("content_md", "")
    draft_file_id_raw = (request.form.get("draft_file_id") or "").strip()
    draft_file_id = int(draft_file_id_raw) if draft_file_id_raw.isdigit() else None

    has_content = bool((content_md or "").strip())
    if not has_content and draft_file_id is None:
        return jsonify({"ok": True, "skipped": True})

    if not title:
        title = f"Untitled {datetime.now().strftime('%Y%m%d-%H%M%S')}"

    try:
        if draft_file_id is not None:
            ok = update_file(draft_file_id, title, content_md, tags, folder_id=folder_id)
            return jsonify({"ok": ok, "file_id": draft_file_id, "created": False})

        file_id = create_file(folder_id, title, content_md, tags)
        return jsonify({"ok": True, "file_id": file_id, "created": True, "title": title})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@sb_bp.get("/sb/file/<int:file_id>")
def sb_file_view(file_id: int):
    file = get_file(file_id)
    if not file:
        return render_template("shared/error.html"), 404
    html_content = render_markdown_safely(file.get("content_md") or "")
    public_url = None
    if file.get("is_public") == "Y" and file.get("public_token"):
        public_url = url_for("sb.sb_public_file_view", token=file["public_token"], _external=True)
    file_path = _build_file_path(file)
    return render_template(
        "sb/file_view.html",
        file=file,
        file_path=file_path,
        html_content=html_content,
        links=list_file_links(file_id),
        public_url=public_url,
    )


@sb_bp.get("/sb/public/<string:token>")
def sb_public_file_view(token: str):
    if not is_public_content_globally_enabled():
        return render_template("sb/public_not_found.html"), 404

    file = get_public_file_by_token(token)
    if not file:
        return render_template("sb/public_not_found.html"), 404
    html_content = render_markdown_safely(file.get("content_md") or "")
    return render_template("sb/public_file_view.html", file=file, html_content=html_content)


@sb_bp.post("/sb/file/<int:file_id>/public")
def sb_file_set_public(file_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    make_public = (request.form.get("make_public") or "").strip().lower() in {"1", "true", "yes", "on", "y"}
    regenerate = (request.form.get("regenerate") or "").strip().lower() in {"1", "true", "yes", "on", "y"}

    try:
        file = set_file_public_state(file_id, make_public=make_public, regenerate_token=regenerate)
    except ValueError as exc:
        return render_template("shared/error.html"), 400

    if not file:
        return render_template("shared/error.html"), 404

    return redirect(url_for("sb.sb_file_view", file_id=file_id))


@sb_bp.get("/sb/file/<int:file_id>/edit")
def sb_file_edit_page(file_id: int):
    file = get_file(file_id)
    if not file:
        return render_template("shared/error.html"), 404
    return render_template(
        "sb/file_editor.html",
        mode="edit",
        file=file,
        file_path=_build_file_path(file),
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
            file_path=_build_breadcrumb(folder_id),
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
    delete_file(file_id)
    return redirect(url_for("sb.sb_home"))


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


import re
from datetime import date, datetime
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from .repository import (
    create_bookmark,
    delete_bookmark,
    get_bookmark,
    list_bookmarks,
    switch_bookmark_starred,
    update_bookmark,
)

bookmarks_bp = Blueprint("bookmarks", __name__)


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


def _extract_title_from_url(url: str) -> str | None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }

    bad_patterns = (
        "just a moment",
        "attention required",
        "access denied",
        "please wait",
        "enable javascript",
        "robot check",
        "captcha",
        "sign in",
        "login",
        "human verification",
    )

    def _clean(text: str | None) -> str | None:
        if not text:
            return None
        cleaned = re.sub(r"\s+", " ", text).strip()
        if not cleaned:
            return None
        if len(cleaned) < 3:
            return None
        return cleaned[:500]

    def _score(text: str, source: str) -> int:
        base = {
            "og:title": 100,
            "twitter:title": 92,
            "title": 84,
            "h1": 58,
        }.get(source, 40)

        lowered = text.lower()
        if any(p in lowered for p in bad_patterns):
            base -= 60
        if lowered.endswith("- google search") or lowered.startswith("search -"):
            base -= 45
        if len(text) > 160:
            base -= 10
        return base

    def _fetch(target_url: str):
        try:
            resp = requests.get(target_url, headers=headers, timeout=(4, 8), allow_redirects=True)
            if not resp.ok:
                return None
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" not in content_type:
                return None
            html = (resp.text or "")[:300000]
            return {"url": resp.url, "html": html}
        except requests.RequestException:
            return None

    page = _fetch(url)
    if not page:
        return None

    soup = BeautifulSoup(page["html"], "html.parser")

    canonical_tag = soup.find("link", rel=lambda v: v and "canonical" in str(v).lower())
    if canonical_tag and canonical_tag.get("href"):
        canonical_url = urljoin(page["url"], canonical_tag.get("href").strip())
        if canonical_url and canonical_url != page["url"]:
            canonical_page = _fetch(canonical_url)
            if canonical_page:
                page = canonical_page
                soup = BeautifulSoup(page["html"], "html.parser")

    candidates: list[tuple[str, str]] = []

    og = soup.find("meta", attrs={"property": "og:title"})
    if og:
        val = _clean(og.get("content"))
        if val:
            candidates.append((val, "og:title"))

    tw = soup.find("meta", attrs={"name": "twitter:title"}) or soup.find("meta", attrs={"property": "twitter:title"})
    if tw:
        val = _clean(tw.get("content"))
        if val:
            candidates.append((val, "twitter:title"))

    if soup.title:
        val = _clean(soup.title.get_text(" ", strip=True))
        if val:
            candidates.append((val, "title"))

    h1 = soup.find("h1")
    if h1:
        val = _clean(h1.get_text(" ", strip=True))
        if val:
            candidates.append((val, "h1"))

    if not candidates:
        return None

    dedup: dict[str, int] = {}
    for text, source in candidates:
        score = _score(text, source)
        dedup[text] = max(score, dedup.get(text, -10_000))

    best = max(dedup.items(), key=lambda kv: kv[1])
    if best[1] < 10:
        return None
    return best[0]


def _fallback_title_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]

    path = (parsed.path or "").strip("/")
    last_segment = ""
    if path:
        last_segment = unquote(path.split("/")[-1]).replace("-", " ").replace("_", " ").strip()

    if host and last_segment:
        return f"{host} / {last_segment}"[:500]
    if host:
        return host[:500]
    return (url or "Untitled Bookmark")[:500]


@bookmarks_bp.get("/bookmarks")
def bookmarks_list():
    search = request.args.get("q", "")
    category = request.args.get("category", "")
    starred = request.args.get("starred", "all")
    page = max(1, int(request.args.get("page", "1")))
    page_size = 20

    items, total_pages, starred = list_bookmarks(
        search=search,
        category=category,
        starred=starred,
        page=page,
        page_size=page_size,
    )
    for item in items:
        item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
        item["created_at_human"] = _humanize_timestamp(item.get("created_at"))

    return render_template(
        "bookmarks/list.html",
        items=items,
        search=search,
        category=category,
        starred=starred,
        page=page,
        total_pages=total_pages,
    )


@bookmarks_bp.get("/bookmarks/new")
def bookmarks_new_page():
    return render_template("bookmarks/form.html", mode="create", item=None, error_message=None)


@bookmarks_bp.post("/bookmarks/new")
def bookmarks_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template(
            "bookmarks/form.html",
            mode="create",
            item=None,
            error_message="Session expired. Please try again.",
        ), 400

    url = (request.form.get("url") or "").strip()
    title = (request.form.get("title") or "").strip()
    category = request.form.get("category") or ""
    starred = request.form.get("starred") or "0"
    notes = request.form.get("notes") or ""

    if not title and url:
        title = _extract_title_from_url(url) or _fallback_title_from_url(url)

    item = {
        "url": url,
        "title": title,
        "category": category,
        "starred": int(starred) if str(starred).strip().isdigit() else 0,
        "notes": notes,
    }

    try:
        bookmark_id = create_bookmark(url, title, category, starred, notes)
        return redirect(url_for("bookmarks.bookmarks_detail", bookmark_id=bookmark_id, msg="created"))
    except ValueError as exc:
        return render_template("bookmarks/form.html", mode="create", item=item, error_message=str(exc)), 400


@bookmarks_bp.get("/bookmarks/<int:bookmark_id>")
def bookmarks_detail(bookmark_id: int):
    item = get_bookmark(bookmark_id)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("bookmarks/detail.html", item=item, message=request.args.get("msg"))


@bookmarks_bp.get("/bookmarks/<int:bookmark_id>/edit")
def bookmarks_edit_page(bookmark_id: int):
    item = get_bookmark(bookmark_id)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("bookmarks/form.html", mode="edit", item=item, error_message=None)


@bookmarks_bp.post("/bookmarks/<int:bookmark_id>/edit")
def bookmarks_edit_submit(bookmark_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    url = (request.form.get("url") or "").strip()
    title = (request.form.get("title") or "").strip()
    category = request.form.get("category") or ""
    starred = request.form.get("starred") or "0"
    notes = request.form.get("notes") or ""

    if not title and url:
        title = _extract_title_from_url(url) or _fallback_title_from_url(url)

    item = {
        "bookmark_id": bookmark_id,
        "url": url,
        "title": title,
        "category": category,
        "starred": int(starred) if str(starred).strip().isdigit() else 0,
        "notes": notes,
    }

    try:
        ok = update_bookmark(bookmark_id, url, title, category, starred, notes)
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("bookmarks.bookmarks_detail", bookmark_id=bookmark_id, msg="updated"))
    except ValueError as exc:
        return render_template("bookmarks/form.html", mode="edit", item=item, error_message=str(exc)), 400


@bookmarks_bp.post("/bookmarks/<int:bookmark_id>/delete")
def bookmarks_delete(bookmark_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_bookmark(bookmark_id)
    return redirect(url_for("bookmarks.bookmarks_list", msg="deleted"))


@bookmarks_bp.post("/bookmarks/<int:bookmark_id>/star")
def bookmarks_switch_star(bookmark_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "Session expired"}), 400

    desired = (request.form.get("starred") or "").strip()
    if desired not in {"0", "1"}:
        return jsonify({"ok": False, "error": "Invalid starred value"}), 400

    ok = switch_bookmark_starred(bookmark_id, desired)
    if not ok:
        return jsonify({"ok": False, "error": "Bookmark not found"}), 404
    return jsonify({"ok": True, "starred": int(desired)})

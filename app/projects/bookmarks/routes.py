import re
import threading
from datetime import date, datetime
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from . import llm
from .repository import (
    create_study_card_job,
    count_study_cards,
    count_uncategorized_bookmarks,
    create_bookmark,
    delete_bookmark,
    fail_study_card_job,
    get_latest_study_card_job,
    get_study_card_job_by_id,
    get_study_card_job_detail,
    get_bookmark,
    list_study_card_jobs,
    list_bookmarks,
    list_study_cards,
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

    def _clean_youtube_url(raw_url: str) -> str:
        parsed = urlparse(raw_url)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]

        video_id = None
        if host in {"youtube.com", "m.youtube.com"}:
            video_id = (parse_qs(parsed.query).get("v") or [None])[0]
        elif host == "youtu.be":
            video_id = parsed.path.strip("/") or None

        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
        return raw_url

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

    def _youtube_title(target_url: str) -> str | None:
        normalized = _clean_youtube_url(target_url)
        parsed = urlparse(normalized)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host not in {"youtube.com", "m.youtube.com", "youtu.be"}:
            return None

        oembed_url = "https://www.youtube.com/oembed"
        try:
            resp = requests.get(
                oembed_url,
                params={"url": normalized, "format": "json"},
                headers=headers,
                timeout=(4, 8),
            )
            if not resp.ok:
                return None
            payload = resp.json()
            return _clean(payload.get("title"))
        except (requests.RequestException, ValueError, TypeError):
            return None

    yt_title = _youtube_title(url)
    if yt_title:
        return yt_title

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


def _parse_quick_bookmark_input(raw_text: str) -> tuple[str, str]:
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Please enter bookmark text")

    m = re.search(r"https?://\S+", text)
    if not m:
        raise ValueError("Quick add expects a valid URL (http/https)")

    url = m.group(0).rstrip(".,;)")
    remainder = (text[:m.start()] + " " + text[m.end():]).strip()
    title = re.sub(r"\s+", " ", remainder).strip()
    return url, title


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

    uncategorized_count = count_uncategorized_bookmarks()

    return render_template(
        "bookmarks/list.html",
        items=items,
        search=search,
        category=category,
        starred=starred,
        page=page,
        total_pages=total_pages,
        uncategorized_count=uncategorized_count,
    )


@bookmarks_bp.post("/bookmarks/categorize")
def bookmarks_categorize_uncategorized():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return redirect(url_for("bookmarks.bookmarks_list", msg="Session expired. Please try again."))

    try:
        result = llm.process_uncategorized_bookmarks(limit=10)
        processed = int(result.get("processed") or 0)
        failed = int(result.get("failed") or 0)
        remaining = int(result.get("remaining") or 0)
        if failed == 0:
            msg = f"Bookmark categorization complete: processed {processed}, remaining {remaining}."
        else:
            msg = f"Bookmark categorization finished: processed {processed}, failed {failed}, remaining {remaining}."
        return redirect(url_for("bookmarks.bookmarks_list", msg=msg))
    except ValueError as exc:
        return redirect(url_for("bookmarks.bookmarks_list", msg=str(exc)))
    except Exception:
        return redirect(url_for("bookmarks.bookmarks_list", msg="Bookmark categorization failed"))


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


@bookmarks_bp.post("/bookmarks/quick-add")
def bookmarks_quick_add_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return redirect(url_for("bookmarks.bookmarks_list", msg="Session expired. Please try again."))

    raw_text = request.form.get("raw_text") or ""
    try:
        url, title = _parse_quick_bookmark_input(raw_text)
        if not title:
            title = _extract_title_from_url(url) or _fallback_title_from_url(url)
        create_bookmark(url=url, title=title, category="", starred=0, notes="")
        return redirect(url_for("bookmarks.bookmarks_list", msg="Quick bookmark added"))
    except ValueError as exc:
        return redirect(url_for("bookmarks.bookmarks_list", msg=str(exc)))
    except Exception:
        return redirect(url_for("bookmarks.bookmarks_list", msg="Failed to add quick bookmark"))


@bookmarks_bp.get("/bookmarks/<int:bookmark_id>")
def bookmarks_detail(bookmark_id: int):
    item = get_bookmark(bookmark_id)
    if not item:
        return render_template("shared/error.html"), 404
    latest_job = get_latest_study_card_job(bookmark_id)
    ask_replace = (request.args.get("ask_replace") or "").strip() in {"1", "true", "yes"}
    return render_template(
        "bookmarks/detail.html",
        item=item,
        latest_job=latest_job,
        message=request.args.get("msg"),
        ask_replace=ask_replace,
    )


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


@bookmarks_bp.post("/bookmarks/<int:bookmark_id>/study-cards/create")
def bookmarks_create_study_cards(bookmark_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_bookmark(bookmark_id)
    if not item:
        return render_template("shared/error.html"), 404

    existing_count = count_study_cards(bookmark_id)
    existing_action = (request.form.get("existing_action") or "").strip().lower()

    if existing_count > 0 and existing_action not in {"replace", "keep"}:
        return redirect(
            url_for(
                "bookmarks.bookmarks_detail",
                bookmark_id=bookmark_id,
                ask_replace=1,
                msg="Study cards already exist. Choose Replace or Keep.",
            )
        )

    if existing_count > 0 and existing_action == "keep":
        return redirect(
            url_for(
                "bookmarks.bookmark_study_cards_view",
                bookmark_id=bookmark_id,
                msg="Kept existing study cards.",
            )
        )

    latest_job = get_latest_study_card_job(bookmark_id)
    if latest_job and latest_job.get("status") in {"QUEUED", "RUNNING"}:
        return redirect(
            url_for(
                "bookmarks.bookmarks_detail",
                bookmark_id=bookmark_id,
                msg="Study card generation is already in progress.",
            )
        )

    try:
        job_id = create_study_card_job(bookmark_id)
        threading.Thread(
            target=llm.process_study_card_job,
            args=(job_id,),
            daemon=True,
            name=f"bookmark-study-job-{job_id}",
        ).start()
        return redirect(
            url_for(
                "bookmarks.bookmarks_detail",
                bookmark_id=bookmark_id,
                msg="Study card generation started. You can check status here and view cards once ready.",
            )
        )
    except ValueError as exc:
        return redirect(url_for("bookmarks.bookmarks_detail", bookmark_id=bookmark_id, msg=str(exc)))
    except Exception:
        return redirect(url_for("bookmarks.bookmarks_detail", bookmark_id=bookmark_id, msg="Failed to start study card generation"))


@bookmarks_bp.get("/bookmarks/<int:bookmark_id>/study-cards/job-status")
def bookmarks_study_cards_job_status(bookmark_id: int):
    item = get_bookmark(bookmark_id)
    if not item:
        return jsonify({"ok": False, "error": "Bookmark not found"}), 404

    latest_job = get_latest_study_card_job(bookmark_id)
    card_count = count_study_cards(bookmark_id)
    return jsonify(
        {
            "ok": True,
            "status": (latest_job or {}).get("status"),
            "error_message": (latest_job or {}).get("error_message"),
            "card_count": card_count,
            "has_cards": card_count > 0,
            "view_url": url_for("bookmarks.bookmark_study_cards_view", bookmark_id=bookmark_id),
        }
    )


@bookmarks_bp.get("/bookmarks/<int:bookmark_id>/study-cards")
def bookmark_study_cards_view(bookmark_id: int):
    item = get_bookmark(bookmark_id)
    if not item:
        return render_template("shared/error.html"), 404

    cards = list_study_cards(bookmark_id)
    if not cards:
        return redirect(url_for("bookmarks.bookmarks_detail", bookmark_id=bookmark_id, msg="No study cards found"))

    return render_template(
        "bookmarks/study_cards.html",
        item=item,
        cards=cards,
        message=request.args.get("msg"),
    )


@bookmarks_bp.get("/bookmarks/study-card-jobs")
def bookmarks_study_card_jobs_list():
    page = max(1, int(request.args.get("page", "1")))
    bookmark_raw = (request.args.get("bookmark_id") or "").strip()
    bookmark_id = int(bookmark_raw) if bookmark_raw.isdigit() else None

    items, total_pages = list_study_card_jobs(page=page, page_size=25, bookmark_id=bookmark_id)
    for item in items:
        item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
        item["started_at_human"] = _humanize_timestamp(item.get("started_at"))
        item["finished_at_human"] = _humanize_timestamp(item.get("finished_at"))

    return render_template(
        "bookmarks/jobs_list.html",
        items=items,
        page=page,
        total_pages=total_pages,
        bookmark_id=bookmark_raw,
    )


@bookmarks_bp.get("/bookmarks/study-card-jobs/<int:job_id>")
def bookmarks_study_card_job_detail(job_id: int):
    item = get_study_card_job_detail(job_id)
    if not item:
        return render_template("shared/error.html"), 404

    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    item["started_at_human"] = _humanize_timestamp(item.get("started_at"))
    item["finished_at_human"] = _humanize_timestamp(item.get("finished_at"))
    item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    return render_template("bookmarks/job_detail.html", item=item)


@bookmarks_bp.post("/bookmarks/study-card-jobs/<int:job_id>/mark-failed")
def bookmarks_mark_study_card_job_failed(job_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return redirect(url_for("bookmarks.bookmarks_study_card_jobs_list", msg="Session expired. Please try again."))

    job = get_study_card_job_by_id(job_id)
    if not job:
        return redirect(url_for("bookmarks.bookmarks_study_card_jobs_list", msg="Job not found"))

    status = (job.get("status") or "").upper()
    if status in {"COMPLETED", "FAILED"}:
        return redirect(
            url_for(
                "bookmarks.bookmarks_study_card_job_detail",
                job_id=job_id,
                msg=f"Job already in terminal status: {status}",
            )
        )

    reason = (request.form.get("reason") or "").strip() or "Manually marked as failed by user"
    fail_study_card_job(job_id, reason)
    return redirect(
        url_for(
            "bookmarks.bookmarks_study_card_job_detail",
            job_id=job_id,
            msg="Job marked as FAILED manually",
        )
    )

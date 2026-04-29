import hashlib
import json
import re
import time
from typing import Any
from urllib.parse import urlparse

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
import requests
from bs4 import BeautifulSoup

from app.projects.ft import repository as ft_repository

from . import repository


class BookmarkCategoryItem(BaseModel):
    bookmark_id: int = Field(description="Bookmark id from input")
    category: str = Field(description="Short category label (1-100 chars)")


class BookmarkCategoryBatch(BaseModel):
    items: list[BookmarkCategoryItem] = Field(description="Output rows mapped by bookmark_id")


class StudyCardItem(BaseModel):
    question: str = Field(description="Direct study prompt/question")
    answer: str = Field(description="Clear, concise learning answer")
    source_excerpt: str | None = Field(default=None, description="Optional short grounding excerpt from source content")


class StudyCardBatch(BaseModel):
    cards: list[StudyCardItem] = Field(description="Generated flash cards")


def _clean_category(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    text = text[:100]
    return text


def _build_prompt(bookmarks: list[dict]) -> str:
    parser = PydanticOutputParser(pydantic_object=BookmarkCategoryBatch)
    prompt = PromptTemplate(
        template=(
            "You are classifying bookmarks into concise categories.\n"
            "Rules:\n"
            "1) Return exactly one item for each input bookmark_id.\n"
            "2) category must be short (1-3 words), practical, and generic (e.g. docs, ai, finance, shopping, tools).\n"
            "3) Use URL + title signals only.\n"
            "4) Do not skip any bookmark_id.\n"
            "Input JSON: {bookmarks_json}\n"
            "{format_instructions}"
        ),
        input_variables=["bookmarks_json"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    return prompt.format(bookmarks_json=json.dumps(bookmarks, ensure_ascii=False))


def process_uncategorized_bookmarks(limit: int = 10) -> dict[str, Any]:
    try:
        from google import genai
    except ImportError as exc:
        raise ValueError("google-genai package is required for bookmark categorization") from exc

    bookmarks = repository.list_uncategorized_bookmarks(limit=min(max(1, int(limit)), 10))
    if not bookmarks:
        remaining = repository.count_uncategorized_bookmarks()
        return {
            "queued": 0,
            "processed": 0,
            "failed": 0,
            "remaining": remaining,
            "model": None,
        }

    api_key = ft_repository.get_google_llm_api_key()
    model_name = ft_repository.get_ft_fast_model_name() or ft_repository.get_ft_model_name()
    client = genai.Client(api_key=api_key, vertexai=False)

    parser = PydanticOutputParser(pydantic_object=BookmarkCategoryBatch)
    prompt = _build_prompt(
        [
            {
                "bookmark_id": int(b["bookmark_id"]),
                "url": b.get("url") or "",
                "title": b.get("title") or "",
            }
            for b in bookmarks
        ]
    )

    started = time.perf_counter()
    response = client.models.generate_content(model=model_name, contents=prompt)
    latency_ms = int((time.perf_counter() - started) * 1000)
    response_text = getattr(response, "text", None) or ""

    usage = getattr(response, "usage_metadata", None)
    prompt_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
    completion_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
    total_tokens = int(getattr(usage, "total_token_count", 0) or 0)

    parsed = parser.parse(response_text).model_dump()
    parsed_by_id = {int(i["bookmark_id"]): i for i in (parsed.get("items") or [])}

    processed = 0
    failed = 0
    outcomes: list[dict[str, Any]] = []

    for row in bookmarks:
        bid = int(row["bookmark_id"])
        item = parsed_by_id.get(bid)
        if not item:
            failed += 1
            outcomes.append({"bookmark_id": bid, "status": "FAILED", "error": "Missing bookmark_id in LLM response"})
            continue

        category = _clean_category(str(item.get("category") or ""))
        if not category:
            failed += 1
            outcomes.append({"bookmark_id": bid, "status": "FAILED", "error": "Empty category"})
            continue

        try:
            ok = repository.update_bookmark_category(bid, category)
            if ok:
                processed += 1
                outcomes.append({"bookmark_id": bid, "status": "PROCESSED", "category": category})
            else:
                failed += 1
                outcomes.append({"bookmark_id": bid, "status": "FAILED", "error": "Bookmark not found"})
        except Exception as exc:
            failed += 1
            outcomes.append({"bookmark_id": bid, "status": "FAILED", "error": str(exc)})

    request_hash = hashlib.sha256(f"bookmarks:{model_name}:{prompt}".encode("utf-8")).hexdigest()
    ft_repository.log_llm_call(
        transaction_id=None,
        model_name=f"bookmarks:{model_name}",
        request_payload=prompt,
        response_payload=response_text,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        latency_ms=latency_ms,
        http_status=200,
        cache_hit=False,
        error_message=None,
        request_hash=request_hash,
        normalized_result_json=json.dumps({"batch_outcomes": outcomes}, ensure_ascii=False),
    )

    remaining = repository.count_uncategorized_bookmarks()
    return {
        "queued": len(bookmarks),
        "processed": processed,
        "failed": failed,
        "remaining": remaining,
        "model": model_name,
    }


def _is_study_cards_unsupported_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host == "youtu.be" or host.endswith("youtube.com") or host.endswith("instagram.com")


def _extract_readable_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    pieces: list[str] = []
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if title:
        pieces.append(f"Title: {title}")

    for node in soup.find_all(["h1", "h2", "h3", "p", "li"]):
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
        if len(text) < 30:
            continue
        pieces.append(text)
        if len("\n".join(pieces)) >= 18000:
            break

    content = "\n".join(pieces)
    return content[:18000]


def _fetch_bookmark_study_source(url: str) -> tuple[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=(5, 12), allow_redirects=True)
    if not resp.ok:
        raise ValueError("Unable to fetch bookmark content")

    content_type = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" not in content_type:
        raise ValueError("Only HTML pages are supported for study cards")

    text = _extract_readable_text_from_html(resp.text or "")
    if len(text) < 300:
        raise ValueError("Not enough readable content found to generate study cards")
    return resp.url, text


def _build_study_cards_prompt(url: str, title: str, notes: str | None, content_text: str, max_cards: int) -> str:
    parser = PydanticOutputParser(pydantic_object=StudyCardBatch)
    prompt = PromptTemplate(
        template=(
            "You are creating study flash cards from a web article/bookmark.\n"
            "Rules:\n"
            "1) Create up to {max_cards} cards, no more.\n"
            "2) Focus on high-signal learning points, definitions, concepts, processes, and practical takeaways.\n"
            "3) question must be crisp and specific.\n"
            "4) answer must be informative and easy to revise from.\n"
            "5) Avoid fluff and duplicate cards.\n"
            "6) Use source_excerpt optionally and keep it short.\n"
            "Bookmark URL: {url}\n"
            "Bookmark Title: {title}\n"
            "Bookmark Notes: {notes}\n"
            "Source Content:\n{content_text}\n"
            "{format_instructions}"
        ),
        input_variables=["url", "title", "notes", "content_text", "max_cards"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    return prompt.format(
        url=url,
        title=title,
        notes=(notes or "")[:2000],
        content_text=content_text,
        max_cards=max_cards,
    )


def generate_study_cards_for_bookmark(url: str, title: str, notes: str | None, max_cards: int) -> dict[str, Any]:
    if _is_study_cards_unsupported_url(url):
        raise ValueError("Study cards are not supported for YouTube or Instagram URLs")

    try:
        from google import genai
    except ImportError as exc:
        raise ValueError("google-genai package is required for study cards") from exc

    resolved_url, content_text = _fetch_bookmark_study_source(url)
    max_cards = max(1, min(int(max_cards), 20))

    api_key = ft_repository.get_google_llm_api_key()
    model_name = ft_repository.get_ft_model_name()
    client = genai.Client(api_key=api_key, vertexai=False)

    prompt = _build_study_cards_prompt(
        url=resolved_url,
        title=title,
        notes=notes,
        content_text=content_text,
        max_cards=max_cards,
    )

    parser = PydanticOutputParser(pydantic_object=StudyCardBatch)
    started = time.perf_counter()
    response = client.models.generate_content(model=model_name, contents=prompt)
    latency_ms = int((time.perf_counter() - started) * 1000)
    response_text = getattr(response, "text", None) or ""

    usage = getattr(response, "usage_metadata", None)
    prompt_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
    completion_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
    total_tokens = int(getattr(usage, "total_token_count", 0) or 0)

    parsed = parser.parse(response_text).model_dump()
    raw_cards = parsed.get("cards") or []
    cards: list[dict[str, str]] = []
    for card in raw_cards:
        if len(cards) >= max_cards:
            break
        question = re.sub(r"\s+", " ", str(card.get("question") or "")).strip()
        answer = re.sub(r"\s+", " ", str(card.get("answer") or "")).strip()
        source_excerpt = re.sub(r"\s+", " ", str(card.get("source_excerpt") or "")).strip()
        if not question or not answer:
            continue
        cards.append(
            {
                "question": question[:500],
                "answer": answer,
                "source_excerpt": source_excerpt[:1000] if source_excerpt else "",
            }
        )

    if not cards:
        raise ValueError("LLM did not return usable study cards")

    request_hash = hashlib.sha256(f"studycards:{model_name}:{prompt}".encode("utf-8")).hexdigest()
    ft_repository.log_llm_call(
        transaction_id=None,
        model_name=f"bookmarks-study:{model_name}",
        request_payload=prompt,
        response_payload=response_text,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        latency_ms=latency_ms,
        http_status=200,
        cache_hit=False,
        error_message=None,
        request_hash=request_hash,
        normalized_result_json=json.dumps({"cards": cards}, ensure_ascii=False),
    )

    return {
        "cards": cards,
        "model": model_name,
        "resolved_url": resolved_url,
        "latency_ms": latency_ms,
    }


def process_study_card_job(job_id: int) -> None:
    if not repository.claim_study_card_job(job_id):
        return

    try:
        job = repository.get_study_card_job_by_id(job_id)
        if not job:
            repository.fail_study_card_job(job_id, "Study card job not found")
            return

        bookmark_id = int(job["bookmark_id"])
        item = repository.get_bookmark(bookmark_id)
        if not item:
            repository.fail_study_card_job(job_id, "Bookmark not found")
            return

        max_cards = repository.get_bookmark_study_cards_max(default_value=10)
        result = generate_study_cards_for_bookmark(
            url=item.get("url") or "",
            title=item.get("title") or "",
            notes=item.get("notes") or "",
            max_cards=max_cards,
        )
        repository.replace_study_cards(bookmark_id, result.get("cards") or [])
        repository.complete_study_card_job(job_id)
    except Exception as exc:
        repository.fail_study_card_job(job_id, str(exc))

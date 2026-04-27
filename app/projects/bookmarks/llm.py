import hashlib
import json
import re
import time
from typing import Any

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field

from app.projects.ft import repository as ft_repository

from . import repository


class BookmarkCategoryItem(BaseModel):
    bookmark_id: int = Field(description="Bookmark id from input")
    category: str = Field(description="Short category label (1-100 chars)")


class BookmarkCategoryBatch(BaseModel):
    items: list[BookmarkCategoryItem] = Field(description="Output rows mapped by bookmark_id")


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

import json
import time
import warnings
from datetime import date
from typing import Any, Optional, TypedDict

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from . import repository

# Some dependency stacks (notably on newer Python runtimes) can emit a noisy
# Pydantic ArbitraryTypeWarning mentioning "<built-in function any>" during
# dynamic schema generation. This is external to our app typing and does not
# affect FT parsing behavior, so we silence just this specific warning text.
warnings.filterwarnings(
    "ignore",
    message=r".*<built-in function any> is not a Python type.*",
    module=r"pydantic\._internal\._generate_schema",
)


class ParsedTransaction(BaseModel):
    direction: str = Field(description="INCOME or EXPENSE")
    amount: float = Field(description="Absolute amount value. Do not include sign.")
    tx_date: str = Field(description="Date in YYYY-MM-DD format")
    category: str = Field(description="Category of transaction")
    description: str = Field(description="Human-readable transaction description")
    account_name: Optional[str] = Field(default=None, description="Best-matched account name or null")
    is_active: Optional[str] = Field(default=None, description="Y or N if explicitly inferred, else null")


class ParsedBatchItem(ParsedTransaction):
    transaction_id: int = Field(description="Transaction id from input list")


class ParsedBatchResponse(BaseModel):
    items: list[ParsedBatchItem] = Field(description="Parsed output rows mapped by transaction_id")


class FTState(TypedDict, total=False):
    transaction: dict
    accounts: list[dict]
    model_name: str
    request_hash: str
    request_payload: str
    response_payload: str
    parsed: dict
    error_message: str
    cache_hit: bool
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: int
    http_status: int


@tool("update_transaction_details")
def update_transaction_details(
    transaction_id: int,
    amount: float,
    direction: str,
    tx_date: str,
    category: str,
    description: str,
    account_name: str | None,
    is_active: str | None,
) -> str:
    """Persist parsed transaction values to database."""
    account_id = repository.resolve_account_id_by_name(account_name)
    ok = repository.update_transaction_from_llm(
        transaction_id=transaction_id,
        amount=amount,
        direction=direction,
        tx_date=tx_date,
        category=category,
        description=description,
        account_id=account_id,
        is_active=is_active,
    )
    return "updated" if ok else "not_found"


def _build_prompt(raw_text: str, accounts: list[dict]) -> str:
    parser = PydanticOutputParser(pydantic_object=ParsedTransaction)
    prompt = PromptTemplate(
        template=(
            "You are a finance parser. Convert a raw transaction text to structured JSON.\n"
            "Rules:\n"
            "1) if text starts with 'income' => direction INCOME, else EXPENSE.\n"
            "2) amount must be positive absolute number.\n"
            "3) if date missing use today's date.\n"
            "4) if category missing use 'uncategorized'.\n"
            "5) account_name should match one of available account names if possible, else null.\n"
            "6) is_active should be Y/N when confidently inferred; else null.\n"
            "7) Prefer account-type rule for is_active: CREDIT=>N, SAVINGS=>Y.\n"
            "Available accounts: {accounts}\n"
            "Today: {today}\n"
            "Raw text: {raw_text}\n"
            "{format_instructions}"
        ),
        input_variables=["accounts", "today", "raw_text"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    return prompt.format(accounts=json.dumps(accounts, ensure_ascii=False), today=date.today().isoformat(), raw_text=raw_text)


def _build_batch_prompt(transactions: list[dict], accounts: list[dict]) -> str:
    parser = PydanticOutputParser(pydantic_object=ParsedBatchResponse)
    prompt = PromptTemplate(
        template=(
            "You are a highly accurate finance parser. Parse ALL transactions below and return strict JSON only.\n"
            "Rules (must follow for each item):\n"
            "1) Keep transaction_id exactly as provided.\n"
            "2) if raw_text starts with 'income' => direction INCOME, else EXPENSE.\n"
            "3) amount must be positive absolute number (no negative sign in output).\n"
            "4) if date missing use today's date.\n"
            "5) if category missing use 'uncategorized'.\n"
            "6) account_name should match one of available account names if possible, else null.\n"
            "7) is_active should be Y/N when confidently inferred; else null. Prefer CREDIT=>N, SAVINGS=>Y based on matched account_name.\n"
            "8) Return one output row for each input row; do not skip any.\n"
            "Available accounts: {accounts}\n"
            "Today: {today}\n"
            "Input transactions JSON: {transactions_json}\n"
            "{format_instructions}"
        ),
        input_variables=["accounts", "today", "transactions_json"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )
    return prompt.format(
        accounts=json.dumps(accounts, ensure_ascii=False),
        today=date.today().isoformat(),
        transactions_json=json.dumps(transactions, ensure_ascii=False),
    )


def _persist_parsed(transaction_id: int, parsed: dict) -> None:
    update_transaction_details.invoke(
        {
            "transaction_id": int(transaction_id),
            "amount": float(parsed.get("amount") or 0),
            "direction": str(parsed.get("direction") or "EXPENSE").upper(),
            "tx_date": str(parsed.get("tx_date") or date.today().isoformat()),
            "category": str(parsed.get("category") or "uncategorized"),
            "description": str(parsed.get("description") or ""),
            "account_name": parsed.get("account_name"),
            "is_active": parsed.get("is_active"),
        }
    )


def _extract_usage(meta: dict[str, Any] | None) -> tuple[int | None, int | None, int | None]:
    if not isinstance(meta, dict):
        return None, None, None
    usage = meta.get("usage_metadata") or meta.get("token_usage") or {}
    if not isinstance(usage, dict):
        return None, None, None
    in_t = usage.get("input_tokens") or usage.get("prompt_tokens")
    out_t = usage.get("output_tokens") or usage.get("completion_tokens")
    total = usage.get("total_tokens")
    try:
        return int(in_t) if in_t is not None else None, int(out_t) if out_t is not None else None, int(total) if total is not None else None
    except (TypeError, ValueError):
        return None, None, None


def _build_graph(llm_invoke):
    parser = PydanticOutputParser(pydantic_object=ParsedTransaction)

    def check_cache(state: FTState) -> FTState:
        tx = state["transaction"]
        request_hash = repository.llm_cache_key(tx.get("raw_text") or "", state["accounts"], state["model_name"])
        state["request_hash"] = request_hash
        cached = repository.find_cached_llm_result(request_hash, state["model_name"])
        if cached:
            state["parsed"] = cached
            state["cache_hit"] = True
        else:
            state["cache_hit"] = False
        return state

    def maybe_call_model(state: FTState) -> FTState:
        if state.get("cache_hit"):
            return state

        tx = state["transaction"]
        prompt = _build_prompt(tx.get("raw_text") or "", state["accounts"])
        state["request_payload"] = prompt

        llm_result = llm_invoke(prompt)
        state["response_payload"] = llm_result.get("response_payload") or ""
        state["latency_ms"] = int(llm_result.get("latency_ms") or 0)
        state["prompt_tokens"] = int(llm_result.get("prompt_tokens") or 0)
        state["completion_tokens"] = int(llm_result.get("completion_tokens") or 0)
        state["total_tokens"] = int(llm_result.get("total_tokens") or 0)
        state["http_status"] = int(llm_result.get("http_status") or 200)

        parsed = parser.parse(state["response_payload"])
        state["parsed"] = parsed.model_dump()
        return state

    def persist_tx(state: FTState) -> FTState:
        if not state.get("parsed"):
            raise ValueError("No parsed payload")
        tx = state["transaction"]
        parsed = state["parsed"]
        update_transaction_details.invoke(
            {
                "transaction_id": int(tx["transaction_id"]),
                "amount": float(parsed.get("amount") or 0),
                "direction": str(parsed.get("direction") or "EXPENSE").upper(),
                "tx_date": str(parsed.get("tx_date") or date.today().isoformat()),
                "category": str(parsed.get("category") or "uncategorized"),
                "description": str(parsed.get("description") or ""),
                "account_name": parsed.get("account_name"),
                "is_active": parsed.get("is_active"),
            }
        )
        return state

    workflow = StateGraph(FTState)
    workflow.add_node("check_cache", check_cache)
    workflow.add_node("call_model", maybe_call_model)
    workflow.add_node("persist", persist_tx)
    workflow.set_entry_point("check_cache")
    workflow.add_edge("check_cache", "call_model")
    workflow.add_edge("call_model", "persist")
    workflow.add_edge("persist", END)
    return workflow.compile()


def process_pending_transactions(limit: int = 50) -> dict:
    if repository.is_llm_processing_disabled():
        raise ValueError("FT LLM processing is disabled by toggle FT_DISABLE_LLM_PROCESSING")

    try:
        from google import genai
    except ImportError as exc:
        raise ValueError("google-genai package is required for FT LLM processing") from exc

    api_key = repository.get_google_llm_api_key()
    base_model_name = repository.get_ft_model_name()
    fast_model_name = repository.get_ft_fast_model_name()
    model_name = fast_model_name or base_model_name
    effective_limit = min(max(1, int(limit)), repository.get_ft_batch_limit(default=20))
    persist_delay_ms = repository.get_ft_persist_delay_ms(default=0)
    accounts = repository.account_context_payload()
    try:
        # Force Gemini Developer API mode (not Vertex AI) for API-key auth.
        client = genai.Client(api_key=api_key, vertexai=False)
    except Exception as exc:
        raise ValueError(f"Failed to initialize Google GenAI client: {exc}") from exc

    def _invoke_google(prompt_text: str) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            response = client.models.generate_content(model=model_name, contents=prompt_text)
        except Exception as exc:
            msg = str(exc)
            if "API key not valid" in msg or "API_KEY_INVALID" in msg or "INVALID_ARGUMENT" in msg:
                raise ValueError(
                    "Google API key is invalid. Update KV key GOOGLE_LLM_API_KEY with a valid key in item_value."
                ) from exc
            raise
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        text = getattr(response, "text", None)
        if not text:
            text = json.dumps(getattr(response, "candidates", []) or [], ensure_ascii=False)

        usage = getattr(response, "usage_metadata", None)
        prompt_tokens = getattr(usage, "prompt_token_count", None)
        completion_tokens = getattr(usage, "candidates_token_count", None)
        total_tokens = getattr(usage, "total_token_count", None)

        return {
            "response_payload": text,
            "latency_ms": elapsed_ms,
            "prompt_tokens": int(prompt_tokens) if prompt_tokens is not None else 0,
            "completion_tokens": int(completion_tokens) if completion_tokens is not None else 0,
            "total_tokens": int(total_tokens) if total_tokens is not None else 0,
            "http_status": 200,
        }

    pending = repository.list_pending_transactions(limit=effective_limit)
    processed = 0
    failed = 0
    cached = 0

    uncached_entries: list[dict[str, Any]] = []

    # First, resolve cache hits without external LLM calls.
    for tx in pending:
        tx_id = int(tx["transaction_id"])
        req_hash = repository.llm_cache_key(tx.get("raw_text") or "", accounts, model_name)
        cached_result = repository.find_cached_llm_result(req_hash, model_name)
        if not cached_result:
            uncached_entries.append({"tx": tx, "request_hash": req_hash})
            continue

        error_message = None
        try:
            _persist_parsed(tx_id, cached_result)
            processed += 1
            cached += 1
        except Exception as exc:
            failed += 1
            error_message = str(exc)
            repository.mark_transaction_failed(tx_id)

        repository.log_llm_call(
            transaction_id=tx_id,
            model_name=model_name,
            request_payload="",
            response_payload="",
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=0,
            latency_ms=0,
            http_status=200,
            cache_hit=True,
            error_message=error_message,
            request_hash=req_hash,
            normalized_result_json=json.dumps(cached_result, ensure_ascii=False),
        )

    # Then process remaining transactions in one batch LLM call.
    batch_prompt = ""
    batch_response_payload = ""
    batch_prompt_tokens = 0
    batch_completion_tokens = 0
    batch_total_tokens = 0
    batch_latency_ms = 0
    batch_http_status = 200
    parsed_by_tx_id: dict[int, dict[str, Any]] = {}

    if uncached_entries:
        batch_parser = PydanticOutputParser(pydantic_object=ParsedBatchResponse)
        batch_input = [
            {"transaction_id": int(e["tx"]["transaction_id"]), "raw_text": e["tx"].get("raw_text") or ""}
            for e in uncached_entries
        ]
        batch_error_message = None
        batch_outcomes: list[dict[str, Any]] = []

        try:
            batch_prompt = _build_batch_prompt(batch_input, accounts)
            batch_result = _invoke_google(batch_prompt)
            batch_response_payload = batch_result.get("response_payload") or ""
            batch_prompt_tokens = int(batch_result.get("prompt_tokens") or 0)
            batch_completion_tokens = int(batch_result.get("completion_tokens") or 0)
            batch_total_tokens = int(batch_result.get("total_tokens") or 0)
            batch_latency_ms = int(batch_result.get("latency_ms") or 0)
            batch_http_status = int(batch_result.get("http_status") or 200)

            parsed_batch = batch_parser.parse(batch_response_payload).model_dump()
            for item in parsed_batch.get("items") or []:
                try:
                    parsed_by_tx_id[int(item["transaction_id"])] = item
                except Exception:
                    continue
        except Exception as exc:
            batch_error_message = str(exc)
            parsed_by_tx_id = {}

        # Persist each transaction from batch output only (strict bulk mode).
        for entry in uncached_entries:
            tx = entry["tx"]
            tx_id = int(tx["transaction_id"])

            parsed = parsed_by_tx_id.get(tx_id)
            error_message = None

            if parsed is None:
                error_message = "Batch output missing this transaction_id; no per-item fallback in strict bulk mode"

            if parsed is not None:
                try:
                    _persist_parsed(tx_id, parsed)
                    processed += 1
                    batch_outcomes.append({"transaction_id": tx_id, "status": "PROCESSED"})
                except Exception as exc:
                    failed += 1
                    error_message = str(exc)
                    repository.mark_transaction_failed(tx_id)
                    batch_outcomes.append({"transaction_id": tx_id, "status": "FAILED", "error": error_message})
            else:
                failed += 1
                repository.mark_transaction_failed(tx_id)
                batch_outcomes.append({"transaction_id": tx_id, "status": "FAILED", "error": error_message})

            if persist_delay_ms > 0:
                time.sleep(persist_delay_ms / 1000.0)

        # One audit row per external batch call (instead of per transaction).
        batch_request_hash = repository.llm_cache_key(
            json.dumps(batch_input, ensure_ascii=False, sort_keys=True),
            accounts,
            model_name,
        )
        repository.log_llm_call(
            transaction_id=None,
            model_name=model_name,
            request_payload=batch_prompt,
            response_payload=batch_response_payload,
            prompt_tokens=batch_prompt_tokens,
            completion_tokens=batch_completion_tokens,
            total_tokens=batch_total_tokens,
            latency_ms=batch_latency_ms,
            http_status=batch_http_status,
            cache_hit=False,
            error_message=batch_error_message,
            request_hash=batch_request_hash,
            normalized_result_json=json.dumps({"batch_outcomes": batch_outcomes}, ensure_ascii=False),
        )

    return {
        "queued": len(pending),
        "processed": processed,
        "failed": failed,
        "cache_hits": cached,
        "model": model_name,
        "base_model": base_model_name,
        "fast_model": fast_model_name,
        "effective_limit": effective_limit,
    }

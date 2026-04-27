from datetime import date, datetime

from flask import Blueprint, render_template, request

from app.projects.ft.repository import get_llm_call, list_llm_calls

from .repository import get_llm_calls_summary

llm_space_bp = Blueprint("llm_space", __name__)


def _humanize_timestamp(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %I:%M %p")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")
    return str(value)


@llm_space_bp.get("/llm-space")
def llm_calls_list():
    page = max(1, int(request.args.get("page", "1")))
    tx_raw = (request.args.get("transaction_id") or "").strip()
    tx_id = int(tx_raw) if tx_raw.isdigit() else None

    items, total_pages = list_llm_calls(page=page, page_size=20, transaction_id=tx_id)
    for i in items:
        i["created_at_human"] = _humanize_timestamp(i.get("created_at"))

    summary = get_llm_calls_summary()
    return render_template(
        "llm_space/llm_calls_list.html",
        items=items,
        page=page,
        total_pages=total_pages,
        transaction_id=tx_raw,
        summary=summary,
    )


@llm_space_bp.get("/llm-space/<int:call_id>")
def llm_call_detail(call_id: int):
    item = get_llm_call(call_id)
    if not item:
        return render_template("shared/error.html"), 404
    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    return render_template("llm_space/llm_call_detail.html", item=item)


@llm_space_bp.get("/dash_llm_space")
def llm_space_dashboard_entry():
    return llm_calls_list()

import re
from datetime import date, datetime

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from . import llm
from .repository import (
    create_account,
    create_transaction,
    delete_account,
    delete_transaction,
    get_account,
    get_finance_summary,
    get_spend_tracker_data,
    get_llm_call,
    get_transaction,
    list_accounts,
    list_llm_calls,
    list_transactions,
    mark_transaction_pending,
    resolve_account_id_by_name,
    update_account,
    update_transaction,
)

ft_bp = Blueprint("ft", __name__)


def _humanize_timestamp(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %I:%M %p")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")
    return str(value)


def _humanize_date_only(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")
    return str(value)


def _parse_raw(raw_text: str) -> dict:
    text = (raw_text or "").strip()
    lower = text.lower()

    direction = "INCOME" if lower.startswith("income") else "EXPENSE"

    amount_match = re.search(r"(-?\d+(?:\.\d+)?)", text)
    amount = abs(float(amount_match.group(1))) if amount_match else 0.0
    if direction == "EXPENSE":
        amount = -abs(amount)

    date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    tx_date = date_match.group(1) if date_match else date.today().isoformat()

    account_name = None
    from_match = re.search(r"\bfrom\s+([A-Za-z0-9_\-]+)", text, flags=re.IGNORECASE)
    if from_match:
        account_name = from_match.group(1).strip()
    else:
        acc_match = re.search(r"\b([A-Za-z0-9_\-]+)\s+account\b", text, flags=re.IGNORECASE)
        if acc_match:
            account_name = acc_match.group(1).strip()

    account_id = resolve_account_id_by_name(account_name)

    tokens = [t for t in re.split(r"\s+", text) if t]
    category = "uncategorized"
    if direction == "INCOME":
        if len(tokens) > 1:
            category = "income"
    else:
        if tokens:
            first = re.sub(r"[^A-Za-z]", "", tokens[0]).lower()
            category = first or "uncategorized"

    description = text
    return {
        "raw_text": text,
        "amount": amount,
        "direction": direction,
        "tx_date": tx_date,
        "category": category,
        "description": description,
        "account_id": account_id,
        "status": "PENDING",
    }


@ft_bp.get("/ft")
def ft_dashboard():
    summary = get_finance_summary()
    pending_items, _, _, _ = list_transactions(
        search="",
        status="PENDING",
        direction="ALL",
        start_date="",
        end_date="",
        account_id=None,
        page=1,
        page_size=10,
    )
    for i in pending_items:
        i["tx_date_human"] = _humanize_date_only(i.get("tx_date"))
    for i in summary.get("recent", []):
        i["tx_date_human"] = _humanize_date_only(i.get("tx_date"))
    return render_template("ft/dashboard.html", summary=summary, pending_items=pending_items)


@ft_bp.get("/ft/tracker")
def ft_spend_tracker():
    data = get_spend_tracker_data()
    return render_template("ft/tracker.html", data=data)


@ft_bp.post("/ft/transactions/raw-ajax")
def ft_transactions_create_raw_ajax():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "Invalid CSRF token"}), 400

    raw_text = request.form.get("raw_text") or ""
    parsed = _parse_raw(raw_text)
    try:
        tx_id = create_transaction(**parsed)
        return jsonify({"ok": True, "transaction_id": tx_id, "message": "Raw transaction added"})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@ft_bp.get("/ft/transactions")
def ft_transactions_list():
    search = request.args.get("q", "")
    status = request.args.get("status", "all")
    direction = request.args.get("direction", "all")
    account_id_raw = (request.args.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    page = max(1, int(request.args.get("page", "1")))
    items, total_pages, status, direction = list_transactions(
        search,
        status,
        direction,
        start_date,
        end_date,
        account_id,
        page,
        20,
        exclude_pending=True,
    )
    unprocessed_items, _, _, _ = list_transactions(
        search,
        "PENDING",
        direction,
        start_date,
        end_date,
        account_id,
        1,
        50,
    )
    accounts_filter = list_accounts(active_only=True)
    for i in items:
        i["updated_at_human"] = _humanize_timestamp(i.get("updated_at"))
        i["tx_date_human"] = _humanize_date_only(i.get("tx_date"))
    for i in unprocessed_items:
        i["updated_at_human"] = _humanize_timestamp(i.get("updated_at"))
        i["tx_date_human"] = _humanize_date_only(i.get("tx_date"))
    return render_template(
        "ft/transactions_list.html",
        items=items,
        unprocessed_items=unprocessed_items,
        search=search,
        status=status,
        direction=direction,
        accounts_filter=accounts_filter,
        selected_account_id=account_id_raw,
        start_date=start_date,
        end_date=end_date,
        page=page,
        total_pages=total_pages,
        message=request.args.get("msg"),
        error_message=request.args.get("err"),
    )


@ft_bp.post("/ft/transactions/raw")
def ft_transactions_create_raw():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    raw_text = request.form.get("raw_text") or ""
    parsed = _parse_raw(raw_text)
    try:
        create_transaction(**parsed)
        return redirect(url_for("ft.ft_transactions_list", msg="raw_created"))
    except ValueError as exc:
        return redirect(url_for("ft.ft_transactions_list", err=str(exc)))


@ft_bp.post("/ft/transactions/process-pending")
def ft_transactions_process_pending():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    try:
        result = llm.process_pending_transactions(limit=50)
        msg = f"processed:{result['processed']},failed:{result['failed']},cache:{result['cache_hits']}"
        return redirect(url_for("ft.ft_transactions_list", msg=msg))
    except ValueError as exc:
        return redirect(url_for("ft.ft_transactions_list", err=str(exc)))


@ft_bp.post("/ft/transactions/<int:transaction_id>/mark-pending")
def ft_transaction_mark_pending(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    ok = mark_transaction_pending(transaction_id)
    if not ok:
        return render_template("shared/error.html"), 404
    return redirect(url_for("ft.ft_transactions_list", msg="marked_pending"))


@ft_bp.get("/ft/transactions/new")
def ft_transactions_new_page():
    return render_template("ft/transaction_form.html", mode="create", item=None, accounts=list_accounts(active_only=True), error_message=None)


@ft_bp.post("/ft/transactions/new")
def ft_transactions_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "raw_text": (request.form.get("raw_text") or "").strip(),
        "amount": request.form.get("amount") or "0",
        "direction": (request.form.get("direction") or "EXPENSE").upper(),
        "tx_date": request.form.get("tx_date") or "",
        "category": request.form.get("category") or "",
        "description": request.form.get("description") or "",
        "account_id": request.form.get("account_id") or "",
        "status": (request.form.get("status") or "MANUAL").upper(),
    }
    account_id = int(item["account_id"]) if str(item["account_id"]).strip().isdigit() else None

    try:
        tx_id = create_transaction(
            raw_text=item["raw_text"],
            amount=item["amount"],
            direction=item["direction"],
            tx_date=item["tx_date"],
            category=item["category"],
            description=item["description"],
            account_id=account_id,
            status=item["status"],
        )
        return redirect(url_for("ft.ft_transaction_detail", transaction_id=tx_id, msg="created"))
    except ValueError as exc:
        return render_template("ft/transaction_form.html", mode="create", item=item, accounts=list_accounts(active_only=True), error_message=str(exc)), 400


@ft_bp.get("/ft/transactions/<int:transaction_id>")
def ft_transaction_detail(transaction_id: int):
    item = get_transaction(transaction_id)
    if not item:
        return render_template("shared/error.html"), 404
    item["created_at_human"] = _humanize_timestamp(item.get("created_at"))
    item["updated_at_human"] = _humanize_timestamp(item.get("updated_at"))
    item["tx_date_human"] = _humanize_timestamp(item.get("tx_date"))
    return render_template("ft/transaction_detail.html", item=item, message=request.args.get("msg"))


@ft_bp.get("/ft/transactions/<int:transaction_id>/edit")
def ft_transaction_edit_page(transaction_id: int):
    item = get_transaction(transaction_id)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("ft/transaction_form.html", mode="edit", item=item, accounts=list_accounts(active_only=True), error_message=None)


@ft_bp.post("/ft/transactions/<int:transaction_id>/edit")
def ft_transaction_edit_submit(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_transaction(transaction_id)
    if not item:
        return render_template("shared/error.html"), 404

    form_item = {
        "transaction_id": transaction_id,
        "raw_text": (request.form.get("raw_text") or "").strip(),
        "amount": request.form.get("amount") or "0",
        "direction": (request.form.get("direction") or "EXPENSE").upper(),
        "tx_date": request.form.get("tx_date") or "",
        "category": request.form.get("category") or "",
        "description": request.form.get("description") or "",
        "account_id": request.form.get("account_id") or "",
        "status": (request.form.get("status") or "MANUAL").upper(),
    }
    account_id = int(form_item["account_id"]) if str(form_item["account_id"]).strip().isdigit() else None

    try:
        ok = update_transaction(
            transaction_id=transaction_id,
            raw_text=form_item["raw_text"],
            amount=form_item["amount"],
            direction=form_item["direction"],
            tx_date=form_item["tx_date"],
            category=form_item["category"],
            description=form_item["description"],
            account_id=account_id,
            status=form_item["status"],
        )
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("ft.ft_transaction_detail", transaction_id=transaction_id, msg="updated"))
    except ValueError as exc:
        return render_template("ft/transaction_form.html", mode="edit", item=form_item, accounts=list_accounts(active_only=True), error_message=str(exc)), 400


@ft_bp.post("/ft/transactions/<int:transaction_id>/delete")
def ft_transaction_delete(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_transaction(transaction_id)
    return redirect(url_for("ft.ft_transactions_list", msg="deleted"))


@ft_bp.get("/ft/accounts")
def ft_accounts_list():
    items = list_accounts(active_only=False)
    for i in items:
        i["updated_at_human"] = _humanize_timestamp(i.get("updated_at"))
    return render_template("ft/accounts_list.html", items=items, message=request.args.get("msg"))


@ft_bp.get("/ft/llm-calls")
def ft_llm_calls_list():
    return redirect(url_for("llm_space.llm_calls_list", transaction_id=request.args.get("transaction_id", ""), page=request.args.get("page", "1")))


@ft_bp.get("/ft/llm-calls/<int:call_id>")
def ft_llm_call_detail(call_id: int):
    return redirect(url_for("llm_space.llm_call_detail", call_id=call_id))


@ft_bp.get("/ft/accounts/new")
def ft_account_new_page():
    return render_template("ft/account_form.html", mode="create", item=None, error_message=None)


@ft_bp.post("/ft/accounts/new")
def ft_account_new_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    item = {
        "account_name": (request.form.get("account_name") or "").strip(),
        "account_type": (request.form.get("account_type") or "SAVINGS").upper(),
    }
    try:
        create_account(item["account_name"], item["account_type"])
        return redirect(url_for("ft.ft_accounts_list", msg="created"))
    except ValueError as exc:
        return render_template("ft/account_form.html", mode="create", item=item, error_message=str(exc)), 400


@ft_bp.get("/ft/accounts/<int:account_id>/edit")
def ft_account_edit_page(account_id: int):
    item = get_account(account_id)
    if not item:
        return render_template("shared/error.html"), 404
    return render_template("ft/account_form.html", mode="edit", item=item, error_message=None)


@ft_bp.post("/ft/accounts/<int:account_id>/edit")
def ft_account_edit_submit(account_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = {
        "account_id": account_id,
        "account_name": (request.form.get("account_name") or "").strip(),
        "account_type": (request.form.get("account_type") or "SAVINGS").upper(),
        "is_active": (request.form.get("is_active") or "Y").upper(),
    }
    try:
        ok = update_account(account_id, item["account_name"], item["account_type"], item["is_active"])
        if not ok:
            return render_template("shared/error.html"), 404
        return redirect(url_for("ft.ft_accounts_list", msg="updated"))
    except ValueError as exc:
        return render_template("ft/account_form.html", mode="edit", item=item, error_message=str(exc)), 400


@ft_bp.post("/ft/accounts/<int:account_id>/delete")
def ft_account_delete(account_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    delete_account(account_id)
    return redirect(url_for("ft.ft_accounts_list", msg="deleted"))

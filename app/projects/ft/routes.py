import re
from datetime import date, datetime

from flask import Blueprint, jsonify, redirect, render_template, request, url_for

from app.core.auth import is_valid_csrf
from app.core.messenger import send_telegram_text
from app.projects.api.repository import extract_api_key_from_request, find_active_api_key_match, get_api_key_header_name
from app.projects.messenger.repository import create_message_record
from app.projects.uploads.repository import get_read_object_url
from . import llm
from .repository import (
    add_transaction_upload_link,
    create_account,
    create_transaction,
    default_transaction_is_active_for_account,
    delete_account,
    delete_transaction,
    get_account,
    get_finance_summary,
    get_spend_tracker_data,
    get_llm_call,
    get_transaction,
    list_accounts,
    list_llm_calls,
    list_transaction_upload_candidates,
    list_transaction_upload_links,
    list_transactions,
    mark_transaction_pending,
    remove_transaction_upload_link,
    resolve_account_id_by_name,
    toggle_transaction_active,
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

    default_is_active = default_transaction_is_active_for_account(account_id, fallback="Y")
    if "not active" in lower or "inactive" in lower:
        is_active = "N"
    elif " active" in lower:
        is_active = "Y"
    else:
        is_active = default_is_active
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
        "is_active": is_active,
    }


def _split_bulk_lines(raw_bulk_text: str) -> list[str]:
    return [line.strip() for line in (raw_bulk_text or "").splitlines() if line.strip()]


def _require_valid_api_key():
    raw_key = extract_api_key_from_request(request)
    if not raw_key:
        return None, (
            jsonify(
                {
                    "ok": False,
                    "error": "API key missing",
                    "header_name": get_api_key_header_name(),
                }
            ),
            400,
        )

    matched = find_active_api_key_match(raw_key)
    if not matched:
        return None, (jsonify({"ok": False, "error": "Invalid API key"}), 401)

    return matched, None


def _notify_ft_processing(event: str, details: str) -> None:
    """Best-effort Telegram notification + audit log for FT processing calls."""
    text = (details or "").strip()
    message_text = f"[FT] {event}\n{text}".strip()

    status = "FAILED"
    telegram_message_id = None
    telegram_chat_id = None
    http_status = None
    response_payload = None
    error_message = None

    try:
        result = send_telegram_text(message_text)
        status = "SENT" if result.get("ok") else "FAILED"
        telegram_message_id = result.get("telegram_message_id")
        telegram_chat_id = result.get("telegram_chat_id")
        http_status = result.get("http_status")
        response_payload = result.get("response_payload")
        error_message = result.get("error_message")
    except Exception as exc:
        error_message = str(exc)

    try:
        create_message_record(
            message_text=message_text,
            status=status,
            telegram_message_id=telegram_message_id,
            telegram_chat_id=telegram_chat_id,
            http_status=http_status,
            response_payload=response_payload,
            error_message=error_message,
            resend_of_message_id=None,
        )
    except Exception:
        # Notification/audit failures should never break FT transaction processing flow.
        pass


@ft_bp.get("/ft")
def ft_dashboard():
    summary = get_finance_summary()
    pending_items, _, _, _, _ = list_transactions(
        search="",
        status="PENDING",
        direction="ALL",
        active_status="ALL",
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


@ft_bp.get("/ft_tracker")
def ft_spend_tracker_dashboard_alias():
    return redirect(url_for("ft.ft_spend_tracker"))


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
    active_status = request.args.get("active_status", "all")
    account_id_raw = (request.args.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    page = max(1, int(request.args.get("page", "1")))
    items, total_pages, status, direction, active_status = list_transactions(
        search,
        status,
        direction,
        active_status,
        start_date,
        end_date,
        account_id,
        page,
        20,
        exclude_pending=True,
    )
    unprocessed_items, _, _, _, _ = list_transactions(
        search,
        "PENDING",
        direction,
        active_status,
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
        active_status=active_status,
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


@ft_bp.get("/ft/transactions/bulk")
def ft_transactions_bulk_page():
    return render_template(
        "ft/transactions_bulk_form.html",
        item={"raw_bulk_text": ""},
        error_message=request.args.get("err"),
        message=request.args.get("msg"),
    )


@ft_bp.post("/ft/transactions/bulk")
def ft_transactions_bulk_submit():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    raw_bulk_text = request.form.get("raw_bulk_text") or ""
    lines = _split_bulk_lines(raw_bulk_text)
    if not lines:
        return render_template(
            "ft/transactions_bulk_form.html",
            item={"raw_bulk_text": raw_bulk_text},
            error_message="Please enter at least one transaction line.",
            message=None,
        ), 400

    created = 0
    failed = 0
    failures: list[str] = []

    for index, line in enumerate(lines, start=1):
        try:
            parsed = _parse_raw(line)
            create_transaction(**parsed)
            created += 1
        except ValueError as exc:
            failed += 1
            failures.append(f"Line {index}: {exc}")

    if failed == 0:
        return redirect(url_for("ft.ft_transactions_list", msg=f"Bulk add complete: created {created} transaction(s)."))

    err_preview = " | ".join(failures[:5])
    if len(failures) > 5:
        err_preview += f" | ...and {len(failures) - 5} more"
    msg = f"Bulk add partial: created {created}, failed {failed}."
    return render_template(
        "ft/transactions_bulk_form.html",
        item={"raw_bulk_text": raw_bulk_text},
        error_message=err_preview,
        message=msg,
    ), 400


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


@ft_bp.post("/ft/api/process-pending")
def ft_api_process_pending():
    api_key_row, err = _require_valid_api_key()
    if err:
        _notify_ft_processing(event="API Process Pending AUTH_ERROR", details="Invalid or missing API key")
        return err

    limit_raw = (request.args.get("limit") or request.form.get("limit") or "50").strip()
    try:
        limit = max(1, min(int(limit_raw), 200))
    except ValueError:
        _notify_ft_processing(event="API Process Pending ERROR", details="limit must be an integer between 1 and 200")
        return jsonify({"ok": False, "error": "limit must be an integer between 1 and 200"}), 400

    try:
        result = llm.process_pending_transactions(limit=limit)
        summary = get_finance_summary()
        _notify_ft_processing(
            event="API Process Pending SUCCESS",
            details=(
                f"api_key={api_key_row.get('item_key')} processed={result.get('processed')} "
                f"failed={result.get('failed')} cache_hits={result.get('cache_hits')} model={result.get('model')}"
            ),
        )
        return jsonify(
            {
                "ok": True,
                "authenticated_api_key": {
                    "item_key": api_key_row.get("item_key"),
                    "name": api_key_row.get("name"),
                },
                "processing": result,
                "summary": {
                    "total_income": summary.get("total_income"),
                    "total_expense": summary.get("total_expense"),
                    "net_amount": summary.get("net_amount"),
                    "pending_count": summary.get("pending_count"),
                    "today_spend": summary.get("today_spend"),
                    "month_spend": summary.get("month_spend"),
                    "llm_calls_today": summary.get("llm_calls_today"),
                    "llm_calls_total": summary.get("llm_calls_total"),
                },
            }
        )
    except ValueError as exc:
        _notify_ft_processing(event="API Process Pending ERROR", details=str(exc))
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        _notify_ft_processing(event="API Process Pending ERROR", details=str(exc))
        raise


@ft_bp.post("/ft/transactions/<int:transaction_id>/mark-pending")
def ft_transaction_mark_pending(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400
    ok = mark_transaction_pending(transaction_id)
    if not ok:
        return render_template("shared/error.html"), 404
    return redirect(url_for("ft.ft_transactions_list", msg="marked_pending"))


@ft_bp.post("/ft/transactions/<int:transaction_id>/toggle-active")
def ft_transaction_toggle_active(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return jsonify({"ok": False, "error": "Invalid CSRF token"}), 400

    result = toggle_transaction_active(transaction_id)
    if not result:
        return jsonify({"ok": False, "error": "Transaction not found"}), 404

    return jsonify({"ok": True, "transaction_id": result["transaction_id"], "is_active": result["is_active"]})


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
        "is_active": (request.form.get("is_active") or "AUTO").upper(),
    }
    account_id = int(item["account_id"]) if str(item["account_id"]).strip().isdigit() else None

    is_active_value = None if item["is_active"] in {"", "AUTO"} else item["is_active"]

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
            is_active=is_active_value,
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

    linked_uploads = list_transaction_upload_links(transaction_id)
    for upload in linked_uploads:
        upload["is_image"] = (upload.get("content_type") or "").lower().startswith("image/")
        try:
            upload["read_object_url"] = get_read_object_url(upload.get("object_name") or "")
        except ValueError:
            upload["read_object_url"] = None
    upload_candidates = list_transaction_upload_candidates(transaction_id)

    return render_template(
        "ft/transaction_detail.html",
        item=item,
        linked_uploads=linked_uploads,
        upload_candidates=upload_candidates,
        message=request.args.get("msg"),
    )


@ft_bp.post("/ft/transactions/<int:transaction_id>/uploads/link")
def ft_transaction_upload_link(transaction_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_transaction(transaction_id)
    if not item:
        return render_template("shared/error.html"), 404

    upload_id_raw = (request.form.get("upload_id") or "").strip()
    if not upload_id_raw.isdigit():
        return redirect(url_for("ft.ft_transaction_detail", transaction_id=transaction_id))

    try:
        add_transaction_upload_link(transaction_id=transaction_id, upload_id=int(upload_id_raw))
        return redirect(url_for("ft.ft_transaction_detail", transaction_id=transaction_id, msg="upload_linked"))
    except Exception:
        return redirect(url_for("ft.ft_transaction_detail", transaction_id=transaction_id, msg="upload_link_failed"))


@ft_bp.post("/ft/transactions/<int:transaction_id>/uploads/<int:upload_id>/unlink")
def ft_transaction_upload_unlink(transaction_id: int, upload_id: int):
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template("shared/error.html"), 400

    item = get_transaction(transaction_id)
    if not item:
        return render_template("shared/error.html"), 404

    remove_transaction_upload_link(transaction_id=transaction_id, upload_id=upload_id)
    return redirect(url_for("ft.ft_transaction_detail", transaction_id=transaction_id, msg="upload_unlinked"))


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
        "is_active": (request.form.get("is_active") or "AUTO").upper(),
    }
    account_id = int(form_item["account_id"]) if str(form_item["account_id"]).strip().isdigit() else None

    is_active_value = None if form_item["is_active"] in {"", "AUTO"} else form_item["is_active"]

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
            is_active=is_active_value or default_transaction_is_active_for_account(account_id, fallback="Y"),
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

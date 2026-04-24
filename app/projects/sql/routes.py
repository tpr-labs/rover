import os

from flask import Blueprint, render_template, request

from app.core.auth import is_valid_csrf
from app.core.db import execute_sql_explorer_query

sql_bp = Blueprint("sql", __name__)


def sql_write_mode_enabled() -> bool:
    write_enabled = os.environ.get("SQL_UI_WRITE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
    app_env = os.environ.get("APP_ENV", os.environ.get("FLASK_ENV", "prod")).lower()
    return write_enabled and app_env == "dev"


@sql_bp.get("/sql")
def sql_page():
    return render_template(
        "sql/sql.html",
        query_text="SELECT city_name, city_state FROM city_info",
        columns=[],
        rows=[],
        result_message=None,
        error_message=None,
        write_mode_enabled=sql_write_mode_enabled(),
    )


@sql_bp.post("/sql/execute")
def sql_execute():
    if not is_valid_csrf(request.form.get("csrf_token")):
        return render_template(
            "sql/sql.html",
            query_text=request.form.get("query", ""),
            columns=[],
            rows=[],
            result_message=None,
            error_message="Session expired. Please try again.",
            write_mode_enabled=sql_write_mode_enabled(),
        ), 400

    query_text = request.form.get("query", "")
    max_rows = int(os.environ.get("SQL_UI_MAX_ROWS", "100"))
    max_rows = max(1, min(max_rows, 500))

    try:
        result = execute_sql_explorer_query(query_text, max_rows=max_rows)
        return render_template(
            "sql/sql.html",
            query_text=query_text,
            columns=result["columns"],
            rows=result["rows"],
            result_message=result["message"],
            error_message=None,
            write_mode_enabled=sql_write_mode_enabled(),
        )
    except ValueError as exc:
        return render_template(
            "sql/sql.html",
            query_text=query_text,
            columns=[],
            rows=[],
            result_message=None,
            error_message=str(exc),
            write_mode_enabled=sql_write_mode_enabled(),
        ), 400

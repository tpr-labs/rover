import os

from flask import jsonify, redirect, render_template, request, session, url_for

from .auth import get_login_token, get_safe_next_url, is_valid_csrf
from .db import get_db_connection, get_schema, execute_sql_explorer_query
from .keyvalue_repo import create_item, deactivate_item, get_item, list_items, restore_item, update_item


def sql_write_mode_enabled() -> bool:
    write_enabled = os.environ.get("SQL_UI_WRITE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
    app_env = os.environ.get("APP_ENV", os.environ.get("FLASK_ENV", "prod")).lower()
    return write_enabled and app_env == "dev"


def register_routes(app):
    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.route("/login", methods=["GET", "POST"])
    def login():
        error_message = None

        if request.method == "POST":
            if not is_valid_csrf(request.form.get("csrf_token")):
                error_message = "Session expired. Please try again."
            else:
                submitted_token = request.form.get("token", "")
                expected_token = get_login_token()
                import hmac
                import secrets

                if hmac.compare_digest(submitted_token, expected_token):
                    session.clear()
                    session["authenticated"] = True
                    session["csrf_token"] = secrets.token_urlsafe(32)
                    return redirect(get_safe_next_url())
                error_message = "Invalid token"

        return render_template(
            "login.html",
            error_message=error_message,
            next_url=get_safe_next_url(),
        )

    @app.post("/logout")
    def logout():
        if is_valid_csrf(request.form.get("csrf_token")):
            session.clear()
        return redirect(url_for("login"))

    @app.get("/")
    def home():
        return redirect(url_for("cities"))

    @app.get("/cities")
    def cities():
        schema = get_schema()
        limit = int(os.environ.get("CITY_LIMIT", "25"))
        limit = max(1, min(limit, 100))

        sql = f"""
            SELECT city_name, city_state, city_famous_description
            FROM {schema}.city_info
            WHERE ROWNUM <= :limit
            ORDER BY city_name
        """

        rows = []
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"limit": limit})
                for city_name, city_state, description in cur.fetchall():
                    rows.append(
                        {
                            "city_name": city_name,
                            "city_state": city_state,
                            "description": description,
                        }
                    )

        return render_template("cities.html", rows=rows)

    @app.get("/sql")
    def sql_page():
        return render_template(
            "sql.html",
            query_text="SELECT city_name, city_state FROM city_info",
            columns=[],
            rows=[],
            result_message=None,
            error_message=None,
            write_mode_enabled=sql_write_mode_enabled(),
        )

    @app.post("/sql/execute")
    def sql_execute():
        if not is_valid_csrf(request.form.get("csrf_token")):
            return render_template(
                "sql.html",
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
                "sql.html",
                query_text=query_text,
                columns=result["columns"],
                rows=result["rows"],
                result_message=result["message"],
                error_message=None,
                write_mode_enabled=sql_write_mode_enabled(),
            )
        except ValueError as exc:
            return render_template(
                "sql.html",
                query_text=query_text,
                columns=[],
                rows=[],
                result_message=None,
                error_message=str(exc),
                write_mode_enabled=sql_write_mode_enabled(),
            ), 400

    @app.get("/kv")
    def kv_list():
        search = request.args.get("q", "")
        category = request.args.get("category", "")
        status = request.args.get("status", "active")
        page = max(1, int(request.args.get("page", "1")))
        page_size = 20

        items, total_pages, status = list_items(search=search, category=category, status=status, page=page, page_size=page_size)
        return render_template(
            "kv_list.html",
            items=items,
            search=search,
            category=category,
            status=status,
            page=page,
            total_pages=total_pages,
        )

    @app.get("/kv/new")
    def kv_new_page():
        return render_template("kv_form.html", mode="create", item=None, error_message=None)

    @app.post("/kv/new")
    def kv_new_submit():
        if not is_valid_csrf(request.form.get("csrf_token")):
            return render_template("kv_form.html", mode="create", item=None, error_message="Session expired. Please try again."), 400

        item = {
            "item_key": (request.form.get("item_key") or "").strip(),
            "item_value": request.form.get("item_value") or "",
            "additional_info": request.form.get("additional_info") or "",
            "category": request.form.get("category") or "",
        }
        try:
            create_item(item["item_key"], item["item_value"], item["additional_info"], item["category"])
            return redirect(url_for("kv_detail", item_key=item["item_key"], msg="created"))
        except ValueError as exc:
            return render_template("kv_form.html", mode="create", item=item, error_message=str(exc)), 400

    @app.get("/kv/<path:item_key>")
    def kv_detail(item_key: str):
        item = get_item(item_key)
        if not item:
            return render_template("error.html"), 404
        return render_template("kv_detail.html", item=item, message=request.args.get("msg"))

    @app.get("/kv/<path:item_key>/edit")
    def kv_edit_page(item_key: str):
        item = get_item(item_key)
        if not item:
            return render_template("error.html"), 404
        return render_template("kv_form.html", mode="edit", item=item, error_message=None)

    @app.post("/kv/<path:item_key>/edit")
    def kv_edit_submit(item_key: str):
        if not is_valid_csrf(request.form.get("csrf_token")):
            return render_template("error.html"), 400

        item = {
            "item_key": item_key,
            "item_value": request.form.get("item_value") or "",
            "additional_info": request.form.get("additional_info") or "",
            "category": request.form.get("category") or "",
            "is_active": request.form.get("is_active") or "Y",
        }
        try:
            ok = update_item(item_key, item["item_value"], item["additional_info"], item["category"], item["is_active"])
            if not ok:
                return render_template("error.html"), 404
            return redirect(url_for("kv_detail", item_key=item_key, msg="updated"))
        except ValueError as exc:
            return render_template("kv_form.html", mode="edit", item=item, error_message=str(exc)), 400

    @app.post("/kv/<path:item_key>/delete")
    def kv_delete(item_key: str):
        if not is_valid_csrf(request.form.get("csrf_token")):
            return render_template("error.html"), 400
        deactivate_item(item_key)
        return redirect(url_for("kv_list", msg="deactivated"))

    @app.post("/kv/<path:item_key>/restore")
    def kv_restore(item_key: str):
        if not is_valid_csrf(request.form.get("csrf_token")):
            return render_template("error.html"), 400
        restore_item(item_key)
        return redirect(url_for("kv_detail", item_key=item_key, msg="restored"))

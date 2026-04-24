import os

from flask import jsonify, redirect, render_template, request, session, url_for

from .auth import get_login_token, get_safe_next_url, is_valid_csrf
from .db import get_db_connection, get_schema


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

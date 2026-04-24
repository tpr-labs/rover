from flask import Blueprint, redirect, render_template, request, session, url_for

from app.core.auth import get_login_token, get_safe_next_url, is_valid_csrf

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
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
        "auth/login.html",
        error_message=error_message,
        next_url=get_safe_next_url(),
    )


@auth_bp.post("/logout")
def logout():
    if is_valid_csrf(request.form.get("csrf_token")):
        session.clear()
    return redirect(url_for("auth.login"))

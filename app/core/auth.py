import hmac
import os
import secrets

from flask import Flask, current_app, redirect, request, session, url_for


def _default_nav_shortcuts() -> list[dict]:
    return [
        {"key": "dashboard", "title": "Dashboard", "path": "/dashboard", "icon_class": "fa-solid fa-house"},
        {"key": "sb", "title": "Secondary Brain", "path": "/sb", "icon_class": "fa-solid fa-brain"},
        {"key": "kv", "title": "Key Value", "path": "/kv", "icon_class": "fa-solid fa-database"},
        {"key": "shortcuts", "title": "Shortcuts", "path": "/shortcuts", "icon_class": "fa-solid fa-link"},
    ]


def get_login_token() -> str:
    token = os.environ.get("APP_LOGIN_TOKEN")
    if not token:
        raise RuntimeError("Server is not fully configured")
    return token


def is_authenticated() -> bool:
    return bool(session.get("authenticated"))


def get_safe_next_url() -> str:
    next_url = request.args.get("next") or request.form.get("next") or "/"
    if isinstance(next_url, str) and next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return "/"


def get_or_create_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def is_valid_csrf(submitted: str | None) -> bool:
    expected = session.get("csrf_token")
    return bool(expected and submitted and hmac.compare_digest(expected, submitted))


def configure_auth(app: Flask) -> None:
    @app.context_processor
    def inject_auth_context():
        authenticated = is_authenticated()
        nav_shortcuts = []
        nav_apps = []
        try:
            from app.projects.kv.repository import list_dashboard_projects

            nav_apps = list_dashboard_projects()
        except Exception as exc:
            current_app.logger.warning("Failed to load dashboard projects for navbar: %s", exc)
            nav_apps = []

        try:
            from app.projects.shortcuts.repository import list_nav_shortcuts

            nav_shortcuts = list_nav_shortcuts()
        except Exception as exc:
            current_app.logger.warning("Failed to load shortcuts for navbar: %s", exc)
            nav_shortcuts = []

        if not nav_shortcuts:
            nav_shortcuts = [
                {
                    "key": p.get("key"),
                    "title": p.get("title"),
                    "path": p.get("path"),
                    "icon_class": p.get("icon_class"),
                }
                for p in nav_apps
                if p.get("path")
            ]

        if not nav_shortcuts:
            nav_shortcuts = _default_nav_shortcuts()

        return {
            "csrf_token": get_or_create_csrf_token(),
            "is_authenticated": authenticated,
            "nav_shortcuts": nav_shortcuts,
            "nav_apps": nav_apps,
        }


def register_auth_guard(app: Flask) -> None:
    @app.before_request
    def require_authentication():
        public_paths = {"/health", "/login"}
        public_endpoints = {
            "api_project.api_validate_key",
            "ft.ft_api_process_pending",
        }
        if (
            request.path in public_paths
            or request.path.startswith("/static/")
            or request.path.startswith("/sb/public/")
            or request.path.startswith("/api/validate-key")
            or request.path.startswith("/ft/api/")
            or request.endpoint in public_endpoints
        ):
            return None
        if is_authenticated():
            return None
        return redirect(url_for("auth.login", next=request.full_path if request.query_string else request.path))

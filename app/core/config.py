import logging
import os

from flask import Flask


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def configure_app(app: Flask) -> None:
    app.config["PROPAGATE_EXCEPTIONS"] = False
    app.secret_key = os.environ.get("APP_SECRET_KEY")
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = env_bool("SESSION_COOKIE_SECURE", False)

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logging.getLogger("MARKDOWN").setLevel(os.getenv("MARKDOWN_LOG_LEVEL", "INFO"))

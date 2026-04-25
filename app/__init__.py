from flask import Flask, render_template
from werkzeug.exceptions import HTTPException

from .core.auth import configure_auth, register_auth_guard
from .core.config import configure_app
from .projects import register_project_blueprints


def create_app() -> Flask:
    app = Flask(__name__, template_folder="../templates")
    configure_app(app)
    configure_auth(app)
    register_auth_guard(app)
    register_project_blueprints(app)

    @app.errorhandler(404)
    def handle_not_found(err):
        return render_template("shared/error.html"), 404

    @app.errorhandler(HTTPException)
    def handle_http_exception(err):
        if err.code == 404:
            return render_template("shared/error.html"), 404
        app.logger.warning("HTTP exception: %s", err)
        return render_template("shared/error.html"), err.code or 500

    @app.errorhandler(Exception)
    def handle_exception(err):
        app.logger.exception("Unhandled exception occurred")
        return render_template("shared/error.html"), 500

    return app

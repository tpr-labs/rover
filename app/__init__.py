from flask import Flask, render_template

from .auth import configure_auth, register_auth_guard
from .config import configure_app
from .routes import register_routes


def create_app() -> Flask:
    app = Flask(__name__, template_folder="../templates")
    configure_app(app)
    configure_auth(app)
    register_auth_guard(app)
    register_routes(app)

    @app.errorhandler(Exception)
    def handle_exception(err):
        app.logger.exception("Unhandled exception occurred")
        return render_template("error.html"), 500

    return app

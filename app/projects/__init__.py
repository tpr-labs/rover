from flask import Flask

from .api.routes import api_bp
from .auth.routes import auth_bp
from .bookmarks.routes import bookmarks_bp
from .core.routes import core_bp
from .ft.routes import ft_bp
from .kv.routes import kv_bp
from .llm_space.routes import llm_space_bp
from .messenger.routes import messenger_bp
from .shortcuts.routes import shortcuts_bp
from .sb.routes import sb_bp
from .sql.routes import sql_bp
from .toggles.routes import toggles_bp
from .uploads.routes import uploads_bp


def register_project_blueprints(app: Flask) -> None:
    app.register_blueprint(auth_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(core_bp)
    app.register_blueprint(ft_bp)
    app.register_blueprint(llm_space_bp)
    app.register_blueprint(messenger_bp)
    app.register_blueprint(sql_bp)
    app.register_blueprint(kv_bp)
    app.register_blueprint(shortcuts_bp)
    app.register_blueprint(bookmarks_bp)
    app.register_blueprint(toggles_bp)
    app.register_blueprint(uploads_bp)
    app.register_blueprint(sb_bp)

from flask import Flask

from .auth.routes import auth_bp
from .core.routes import core_bp
from .kv.routes import kv_bp
from .sb.routes import sb_bp
from .sql.routes import sql_bp
from .toggles.routes import toggles_bp


def register_project_blueprints(app: Flask) -> None:
    app.register_blueprint(auth_bp)
    app.register_blueprint(core_bp)
    app.register_blueprint(sql_bp)
    app.register_blueprint(kv_bp)
    app.register_blueprint(toggles_bp)
    app.register_blueprint(sb_bp)

import os

from flask import Blueprint, jsonify, redirect, render_template, url_for

from app.core.db import get_db_connection, get_schema
from app.projects.kv.repository import list_dashboard_projects

core_bp = Blueprint("core", __name__)


@core_bp.get("/health")
def health():
    return jsonify({"status": "ok"})


@core_bp.get("/")
def home():
    return redirect(url_for("core.dashboard"))


@core_bp.get("/dashboard")
def dashboard():
    projects = list_dashboard_projects()
    return render_template("dashboard/dashboard.html", projects=projects)


@core_bp.get("/cities")
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

    return render_template("legacy/cities.html", rows=rows)



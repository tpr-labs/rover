import logging
import os
import re
from flask import Flask, jsonify, render_template_string
import oracledb


app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = False

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def get_db_connection():
    wallet_dir = os.environ.get("ORA_WALLET_DIR", "/tmp/wallet")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_dsn = os.environ.get("DB_DSN", "projectxdev_low")
    db_wallet_password = os.environ.get("DB_WALLET_PASSWORD")

    if not db_user or not db_password or not db_wallet_password:
        raise RuntimeError("Server is not fully configured")

    return oracledb.connect(
        user=db_user,
        password=db_password,
        dsn=db_dsn,
        config_dir=wallet_dir,
        wallet_location=wallet_dir,
        wallet_password=db_wallet_password,
    )


def get_schema() -> str:
    schema = os.environ.get("ORA_SCHEMA", "ADMIN")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", schema):
        raise ValueError("Invalid ORA_SCHEMA format")
    return schema


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/")
def home():
    return cities()


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

    template = """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>City Info</title>
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
      <style>
        body { background: #f8fafc; }
        .page-wrap { max-width: 1100px; }
      </style>
    </head>
    <body>
      <div class="container py-4 page-wrap">
        <div class="d-flex justify-content-between align-items-center mb-3">
          <h1 class="h4 mb-0">City Directory</h1>
          <span class="badge text-bg-primary">{{ rows|length }} rows</span>
        </div>
        <div class="card shadow-sm border-0">
          <div class="card-body p-0">
            <div class="table-responsive">
              <table class="table table-hover table-striped mb-0 align-middle">
                <thead class="table-dark">
                  <tr>
                    <th>City</th>
                    <th>State</th>
                    <th>Description</th>
                  </tr>
                </thead>
                <tbody>
                {% for r in rows %}
                  <tr>
                    <td class="fw-semibold">{{ r.city_name }}</td>
                    <td><span class="badge text-bg-light border">{{ r.city_state }}</span></td>
                    <td>{{ r.description }}</td>
                  </tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </body>
    </html>
    """

    return render_template_string(template, rows=rows)


@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception occurred")
    return (
        render_template_string(
            """
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8" />
              <meta name="viewport" content="width=device-width, initial-scale=1" />
              <title>Service Unavailable</title>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
            </head>
            <body class="bg-light">
              <div class="container py-5" style="max-width: 760px;">
                <div class="alert alert-danger" role="alert">
                  <h4 class="alert-heading">Something went wrong</h4>
                  <p class="mb-0">The service is temporarily unavailable. Please try again later.</p>
                </div>
              </div>
            </body>
            </html>
            """
        ),
        500,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)

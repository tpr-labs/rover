#!/usr/bin/env sh
set -eu

echo "[startup] validating required environment variables..."

: "${WALLET_PAR_URL:?WALLET_PAR_URL is required}"
: "${DB_USER:?DB_USER is required}"
: "${DB_PASSWORD:?DB_PASSWORD is required}"
: "${DB_WALLET_PASSWORD:?DB_WALLET_PASSWORD is required}"
: "${APP_LOGIN_TOKEN:?APP_LOGIN_TOKEN is required}"
: "${APP_SECRET_KEY:?APP_SECRET_KEY is required}"

DB_DSN="${DB_DSN:-projectxdev_low}"
ORA_WALLET_DIR="${ORA_WALLET_DIR:-/tmp/wallet}"
PORT="${PORT:-8080}"

echo "[startup] preparing wallet dir: ${ORA_WALLET_DIR}"
rm -rf "${ORA_WALLET_DIR}" /tmp/wallet.zip
mkdir -p "${ORA_WALLET_DIR}"

echo "[startup] downloading wallet zip from WALLET_PAR_URL..."
curl -fsSL "${WALLET_PAR_URL}" -o /tmp/wallet.zip

echo "[startup] extracting wallet..."
unzip -q /tmp/wallet.zip -d "${ORA_WALLET_DIR}"
rm -f /tmp/wallet.zip

export DB_DSN
export ORA_WALLET_DIR
export PORT
export FLASK_ENV=production
export PYTHONUNBUFFERED=1

echo "[startup] launching app with gunicorn on port ${PORT}"
exec gunicorn --bind "0.0.0.0:${PORT}" --workers "${GUNICORN_WORKERS:-2}" --threads "${GUNICORN_THREADS:-4}" --timeout "${GUNICORN_TIMEOUT:-60}" "app:create_app()"

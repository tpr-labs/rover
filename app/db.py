import os
import re

import oracledb


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

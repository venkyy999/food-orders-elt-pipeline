from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine


PROJECT_DIR = Path(__file__).resolve().parent
SOURCE_CSV = PROJECT_DIR / "food_orders.csv"
REPORTS_DIR = PROJECT_DIR / "reports"
BATCH_SIZE = 100
RAW_DATABASE = "food_orders_raw_db"
ANALYTICS_DATABASE = "food_orders_analytics_db"
PIPELINE_VERSION = "1"

load_dotenv(PROJECT_DIR / ".env", override=False)


def _mysql_url(database: str | None = None) -> URL:
    host = os.environ.get("DB_HOST", "").strip()
    user = os.environ.get("DB_USER", "").strip()
    password = os.environ.get("DB_PASSWORD", "")
    port_text = os.environ.get("DB_PORT", "3306").strip()
    missing = [
        key
        for key, value in (
            ("DB_HOST", host),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        )
        if not value or value.lower().startswith("your_mysql_")
    ]
    if missing:
        raise RuntimeError(
            "MySQL credentials are not configured. Set "
            f"{', '.join(missing)} in the project .env file."
        )
    try:
        port = int(port_text)
    except ValueError as exc:
        raise RuntimeError("DB_PORT must be a valid integer.") from exc
    return URL.create(
        "mysql+mysqlconnector",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def create_database_engine(database: str) -> Engine:
    if database not in {RAW_DATABASE, ANALYTICS_DATABASE}:
        raise ValueError(f"Unexpected database name: {database}")
    server_engine = create_engine(
        _mysql_url(),
        pool_pre_ping=True,
        connect_args={"connection_timeout": 5},
    )
    try:
        with server_engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        server_engine.dispose()
    return create_engine(
        _mysql_url(database),
        pool_pre_ping=True,
        connect_args={"connection_timeout": 5},
    )


def load_run_id(source_path: Path = SOURCE_CSV) -> str:
    import hashlib

    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    return f"{PIPELINE_VERSION}-{digest}"

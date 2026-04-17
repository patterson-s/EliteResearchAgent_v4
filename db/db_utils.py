"""Shared database connection utility for prosopography migration scripts."""

import os
import psycopg2
from dotenv import load_dotenv

# Load .env from the v4 project root (one level up from db/)
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(_env_path)


def get_connection(autocommit: bool = False) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection.

    Prefers DATABASE_URL (Render format) if set; falls back to individual
    DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD vars from .env.
    """
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        dsn = database_url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(dsn)
    else:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
    conn.autocommit = autocommit
    return conn

"""Shared database connection utility for prosopography migration scripts."""

import os
import psycopg2
from dotenv import load_dotenv

# Load .env from the v4 project root (one level up from db/)
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(_env_path)


def get_connection(autocommit: bool = False) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection using credentials from .env."""
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    conn.autocommit = autocommit
    return conn

"""Shared DB connection for the web layer. One connection per request (thread-safe)."""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABASE_URL = os.environ.get("DATABASE_URL")


@contextmanager
def get_conn():
    if DATABASE_URL:
        # Render provides postgres:// scheme; psycopg2 requires postgresql://
        dsn = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(dsn)
    else:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
    try:
        yield conn
    finally:
        conn.close()


def row_to_dict(cursor, row) -> dict:
    """Convert a psycopg2 row to a dict using cursor.description."""
    return {col.name: val for col, val in zip(cursor.description, row)}


def rows_to_dicts(cursor) -> list[dict]:
    return [row_to_dict(cursor, row) for row in cursor.fetchall()]

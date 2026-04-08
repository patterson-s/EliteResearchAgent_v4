"""
migrate_21_person_notes.py
---------------------------
Creates a simple free-form notes table for per-person annotations.
One row per person, upserted on save. Safe to re-run.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prosopography.person_notes (
                person_id   INTEGER PRIMARY KEY
                            REFERENCES prosopography.persons(person_id)
                            ON DELETE CASCADE,
                note_text   TEXT NOT NULL DEFAULT '',
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM prosopography.person_notes")
        n = cur.fetchone()[0]
        cur.close()
        print(f"migrate_21: person_notes table ready ({n} existing rows)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

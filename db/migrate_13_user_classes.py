"""
migrate_13_user_classes.py
-----------------------------------
Creates the ontology_user_classes table, which persists user-defined
equivalence class extensions (e.g. 'ministry_of_finance' as a child of
'cabinet') so they can be reused across annotation sessions.

Safe to re-run: uses CREATE TABLE IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS prosopography.ontology_user_classes (
    class_id     SERIAL PRIMARY KEY,
    value        TEXT NOT NULL UNIQUE,   -- slug, e.g. 'ministry_of_finance'
    label        TEXT NOT NULL,          -- display name, e.g. 'Ministry of Finance'
    parent_class TEXT NOT NULL,          -- hardcoded parent, e.g. 'cabinet'
    category     TEXT NOT NULL,          -- annotation category, e.g. 'executive'
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def run():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.close()
    conn.close()
    print("migrate_13: ontology_user_classes table created (or already exists).")


if __name__ == "__main__":
    run()

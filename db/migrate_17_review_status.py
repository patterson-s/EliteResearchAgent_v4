"""
migrate_17_review_status.py
-----------------------------------
Adds review_status TEXT column to org_ontology_mappings to support
the per-item approval workflow in the ontology review mode.

Values: 'pending' (default) | 'approved' | 'flagged'
All existing rows default to 'pending' (unreviewed).

Safe to re-run: uses ADD COLUMN IF NOT EXISTS and CREATE INDEX IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (review_status IN ('pending', 'approved', 'flagged'));

CREATE INDEX IF NOT EXISTS idx_oom_review_status
    ON prosopography.org_ontology_mappings (run_id, review_status);
"""


def run():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'prosopography'
          AND table_name   = 'org_ontology_mappings'
          AND column_name  = 'review_status'
    """)
    exists = cur.fetchone() is not None
    if exists:
        cur.execute("""
            SELECT review_status, COUNT(*)
            FROM prosopography.org_ontology_mappings
            GROUP BY review_status ORDER BY review_status
        """)
        counts = cur.fetchall()
        print(f"migrate_17: review_status column present. Counts: {dict(counts)}")
    else:
        print("migrate_17: review_status column MISSING.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run()

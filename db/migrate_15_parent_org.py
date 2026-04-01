"""
migrate_15_parent_org.py
-----------------------------------
Adds a parent_org TEXT column to org_ontology_mappings for recording
org-to-org sub-unit relationships (e.g., a World Bank panel → World Bank Group).

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS parent_org TEXT;

CREATE INDEX IF NOT EXISTS idx_oom_parent_org
    ON prosopography.org_ontology_mappings (parent_org)
    WHERE parent_org IS NOT NULL;
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
          AND column_name  = 'parent_org'
    """)
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"migrate_15: parent_org column {'present' if exists else 'MISSING'}.")


if __name__ == "__main__":
    run()

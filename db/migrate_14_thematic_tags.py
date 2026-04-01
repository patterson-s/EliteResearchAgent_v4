"""
migrate_14_thematic_tags.py
-----------------------------------
Adds a thematic_tags TEXT[] column to org_ontology_mappings and a GIN index
for efficient array containment queries.

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS thematic_tags TEXT[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_oom_thematic_tags
    ON prosopography.org_ontology_mappings USING GIN(thematic_tags);
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
          AND column_name  = 'thematic_tags'
    """)
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"migrate_14: thematic_tags column {'present' if exists else 'MISSING'}.")


if __name__ == "__main__":
    run()

"""
migrate_16_parent_org_id.py
-----------------------------------
Adds parent_org_id INTEGER FK to org_ontology_mappings, enabling proper
tree traversal of organizational hierarchies captured during annotation.

The parent_org TEXT column (from migrate_15) is retained as the human-readable
source reference used during the resolution pass. parent_org_id is the
normalized FK that enables JOINs and recursive CTE queries.

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS parent_org_id INTEGER
        REFERENCES prosopography.organizations(org_id)
        ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_oom_parent_org_id
    ON prosopography.org_ontology_mappings (parent_org_id)
    WHERE parent_org_id IS NOT NULL;
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
          AND column_name  = 'parent_org_id'
    """)
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"migrate_16: parent_org_id column {'present' if exists else 'MISSING'}.")


if __name__ == "__main__":
    run()

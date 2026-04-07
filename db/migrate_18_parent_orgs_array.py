"""
migrate_18_parent_orgs_array.py
-----------------------------------
Adds parent_orgs TEXT[] to org_ontology_mappings, enabling multi-valued
parent organization recording during annotation.

The existing parent_org TEXT column is retained as the legacy single-value
field (used by the resolution queue FK workflow). parent_orgs is the new
multi-value field written by the annotation form.

Backfill: existing parent_org values are copied into parent_orgs[1].

Safe to re-run: uses ADD COLUMN IF NOT EXISTS; backfill is idempotent.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS parent_orgs TEXT[] DEFAULT ARRAY[]::TEXT[];
"""

BACKFILL = """
UPDATE prosopography.org_ontology_mappings
SET parent_orgs = ARRAY[parent_org]
WHERE parent_org IS NOT NULL
  AND (parent_orgs IS NULL OR array_length(parent_orgs, 1) IS NULL);
"""


def run():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.execute(BACKFILL)
    backfilled = cur.rowcount
    conn.commit()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'prosopography'
          AND table_name   = 'org_ontology_mappings'
          AND column_name  = 'parent_orgs'
    """)
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"migrate_18: parent_orgs column {'present' if exists else 'MISSING'}. Backfilled {backfilled} rows.")


if __name__ == "__main__":
    run()

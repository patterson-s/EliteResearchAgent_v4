"""
migrate_19_alias_of_org.py
-----------------------------------
Adds alias_of_org_id INTEGER FK to org_ontology_mappings, enabling per-run
alias annotation. When equivalence_class = 'alias', this column stores the
org_id of the canonical organization that this org is a duplicate of.

All other annotation fields (hierarchy_path, display_label, etc.) are left
NULL for alias rows — they inherit from the canonical for analytics purposes.

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.org_ontology_mappings
    ADD COLUMN IF NOT EXISTS alias_of_org_id INTEGER
        REFERENCES prosopography.organizations(org_id)
        ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_oom_alias_of_org_id
    ON prosopography.org_ontology_mappings (alias_of_org_id)
    WHERE alias_of_org_id IS NOT NULL;
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
          AND column_name  = 'alias_of_org_id'
    """)
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    print(f"migrate_19: alias_of_org_id column {'present' if exists else 'MISSING'}.")


if __name__ == "__main__":
    run()

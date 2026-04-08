"""
migrate_20_functional_tags.py
-----------------------------------
Creates two tables for user-applied functional tags:

  prosopography.user_functional_tags  — stores tag arrays per entity
  prosopography.functional_tag_vocab  — tracks used tags for autocomplete

Also seeds two derivative_runs rows (one for person-level tags, one for
position-level tags) so the provenance chain is complete.

Safe to re-run: uses CREATE TABLE IF NOT EXISTS and ON CONFLICT DO NOTHING.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS prosopography.user_functional_tags (
    id          SERIAL PRIMARY KEY,
    entity_type TEXT    NOT NULL CHECK (entity_type IN ('person', 'position')),
    entity_id   INTEGER NOT NULL,
    run_id      INTEGER NOT NULL
                    REFERENCES prosopography.derivative_runs(run_id)
                    ON DELETE CASCADE,
    tags        TEXT[]  NOT NULL DEFAULT '{}',
    updated_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_uft_entity
    ON prosopography.user_functional_tags (entity_type, entity_id);

CREATE TABLE IF NOT EXISTS prosopography.functional_tag_vocab (
    tag_name   TEXT PRIMARY KEY,
    use_count  INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

SEED_RUNS = """
INSERT INTO prosopography.derivative_runs
    (run_name, derivative_type, entity_level, evaluation_status, run_timestamp)
VALUES
    ('user_ftags_person_v1',   'user_tagging', 'person',   'draft', now()),
    ('user_ftags_position_v1', 'user_tagging', 'position', 'draft', now())
ON CONFLICT DO NOTHING;
"""


def run():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute(SEED_RUNS)
    conn.commit()

    # Verify
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'prosopography'
          AND table_name IN ('user_functional_tags', 'functional_tag_vocab')
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT run_name FROM prosopography.derivative_runs
        WHERE run_name IN ('user_ftags_person_v1', 'user_ftags_position_v1')
        ORDER BY run_name
    """)
    runs = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()
    print(f"migrate_20: tables present: {tables}")
    print(f"migrate_20: derivative_runs seeded: {runs}")


if __name__ == "__main__":
    run()

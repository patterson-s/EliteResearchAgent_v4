"""
migrate_12_create_org_ontology.py
-----------------------------------
Creates the org_ontology_mappings derivative table and seeds an initial
derivative_runs row for the MFA annotation pass (org_ontology_mfa_v1).

Safe to re-run: uses CREATE TABLE IF NOT EXISTS and INSERT ... ON CONFLICT DO NOTHING.
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
-- ── Org Ontology Mappings ─────────────────────────────────────────────────
-- Derivative layer: maps each organization to a position in a hierarchical,
-- country-agnostic ontology. Non-destructive — base org data is untouched.
-- Phase 1 focus: Ministries of Foreign Affairs and their sub-units.
CREATE TABLE IF NOT EXISTS prosopography.org_ontology_mappings (
    mapping_id        SERIAL PRIMARY KEY,
    org_id            INTEGER     NOT NULL
                      REFERENCES prosopography.organizations(org_id) ON DELETE CASCADE,
    run_id            INTEGER     NOT NULL
                      REFERENCES prosopography.derivative_runs(run_id),

    -- Cross-country equivalence class (what type of org this is)
    equivalence_class TEXT        NOT NULL,
    -- e.g. 'ministry_of_foreign_affairs', 'embassy', 'permanent_mission',
    --      'consulate', 'diplomatic_service', 'national_government', 'not_mfa'

    -- Location / scope
    country_code      TEXT,       -- ISO alpha-3, consistent with existing org data

    -- Hierarchy
    parent_category   TEXT,       -- equivalence_class of the parent
    hierarchy_path    TEXT[],     -- ordered path from root, e.g.
                                  -- ['national_government', 'ministry_of_foreign_affairs']
    display_label     TEXT,       -- human-readable label, e.g. "Ministry of Foreign Affairs (EGY)"

    -- Annotation provenance
    annotated_by      TEXT        NOT NULL DEFAULT 'manual',
    annotation_notes  TEXT,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, run_id)
);

COMMENT ON TABLE prosopography.org_ontology_mappings IS
    'Derivative ontology layer. Maps each organization to a standardized equivalence class '
    'within a country-agnostic hierarchy. Phase 1: MFAs and sub-units (embassies, missions). '
    'Non-destructive: base organizations data is never overwritten.';

COMMENT ON COLUMN prosopography.org_ontology_mappings.equivalence_class IS
    'Cross-country type: ministry_of_foreign_affairs, embassy, permanent_mission, '
    'consulate, diplomatic_service, national_government, not_mfa';

COMMENT ON COLUMN prosopography.org_ontology_mappings.hierarchy_path IS
    'Ordered array from root to this node, e.g. '
    '[''national_government'', ''ministry_of_foreign_affairs'', ''embassy'']';

CREATE INDEX IF NOT EXISTS idx_oom_org_id       ON prosopography.org_ontology_mappings(org_id);
CREATE INDEX IF NOT EXISTS idx_oom_run_id       ON prosopography.org_ontology_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_oom_eq_class     ON prosopography.org_ontology_mappings(equivalence_class);
CREATE INDEX IF NOT EXISTS idx_oom_country      ON prosopography.org_ontology_mappings(country_code);
CREATE INDEX IF NOT EXISTS idx_oom_parent_cat   ON prosopography.org_ontology_mappings(parent_category);
"""

SEED_DERIVATIVE_RUN = """
INSERT INTO prosopography.derivative_runs (
    run_name,
    derivative_type,
    entity_level,
    narrative,
    evaluation_status,
    run_timestamp
)
VALUES (
    'org_ontology_mfa_v1',
    'ontology_mapping',
    'organization',
    'Manual annotation pass — Phase 1: Ministries of Foreign Affairs and sub-units '
    '(embassies, permanent missions, consulates). Establishes cross-country equivalence '
    'classes for MFA-type government organizations.',
    'draft',
    %s
)
ON CONFLICT DO NOTHING;
"""


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        print("Creating org_ontology_mappings table...")
        cur.execute(DDL)

        print("Seeding derivative_runs row for org_ontology_mfa_v1...")
        cur.execute(SEED_DERIVATIVE_RUN, (datetime.now(timezone.utc),))

        conn.commit()

        # Report
        cur.execute("SELECT to_regclass('prosopography.org_ontology_mappings') IS NOT NULL")
        print(f"  org_ontology_mappings: {'OK' if cur.fetchone()[0] else 'MISSING'}")

        cur.execute(
            "SELECT run_id, run_name FROM prosopography.derivative_runs "
            "WHERE run_name = 'org_ontology_mfa_v1'"
        )
        row = cur.fetchone()
        if row:
            print(f"  derivative_runs seed: run_id={row[0]}, run_name={row[1]}")
        else:
            print("  derivative_runs seed: NOT FOUND")

        cur.close()
        print("Done.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

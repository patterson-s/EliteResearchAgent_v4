"""
migrate_06_create_org_schema.py
--------------------------------
Creates the organizations and organization_aliases tables.
Adds org_id FK and org_match_method columns to career_positions.
Safe to re-run (IF NOT EXISTS + idempotent ALTER).
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
-- ──────────────────────────────────────────────────────────────
-- Organizations (canonical entries)
-- Sourced from v3/services/ontology_01/final_ontology.json
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.organizations (
    org_id              SERIAL PRIMARY KEY,
    canonical_name      TEXT     NOT NULL UNIQUE,

    -- Classification
    meta_type           TEXT     CHECK (meta_type IN ('io', 'gov', 'university', 'ngo', 'private', 'other')),
    org_types           TEXT[],       -- specific type tags; first element is primary
    sector              TEXT,

    -- Location
    location_country    TEXT,         -- ISO country code (alpha-2 or alpha-3, as stored in source)
    location_city       TEXT,

    -- UN sub-ontology (56 entries have this)
    un_canonical_tag    TEXT,
    un_hierarchical_tags TEXT[],

    -- GOV sub-ontology (317 entries have this)
    gov_canonical_tag   TEXT,
    gov_hierarchical_tags TEXT[],
    gov_country         TEXT,

    -- Hierarchy (self-referential, text for now; only 1 entry populated)
    parent_org_name     TEXT,

    -- Provenance / review
    source              TEXT,
    review_status       TEXT     CHECK (review_status IN ('completed', 'pending_review', 'merged', 'base')),

    data_status         TEXT     NOT NULL DEFAULT 'base'
                                 CHECK (data_status IN ('base', 'derivative')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE prosopography.organizations IS
    'Canonical organization registry. Sourced from v3 final_ontology.json (1,609 entries). '
    'Part 2 addition: Part 1 career_positions.organization is plain text; this table provides '
    'normalized classifications for org-type-based analysis.';

COMMENT ON COLUMN prosopography.organizations.meta_type IS
    'Analysis-oriented classification: io=international org, gov=national government, '
    'university=academic institution, ngo=NGO/foundation/think tank, private=corporation, other.';

COMMENT ON COLUMN prosopography.organizations.review_status IS
    'Quality flag from v3 ontology pipeline. ~1090 entries are pending_review (auto-stubbed); '
    '~514 NULL entries are original high-quality records (stored as base).';

-- ──────────────────────────────────────────────────────────────
-- Organization aliases / name variations
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.organization_aliases (
    alias_id    SERIAL PRIMARY KEY,
    org_id      INTEGER NOT NULL REFERENCES prosopography.organizations(org_id) ON DELETE CASCADE,
    alias       TEXT    NOT NULL,
    UNIQUE (alias)
);

COMMENT ON TABLE prosopography.organization_aliases IS
    'Alternative names, acronyms, and spelling variants for canonical organizations. '
    'Each alias maps to exactly one org. Used for org_id resolution during matching.';
"""

ALTER_CAREER_POSITIONS = """
ALTER TABLE prosopography.career_positions
    ADD COLUMN IF NOT EXISTS org_id           INTEGER REFERENCES prosopography.organizations(org_id),
    ADD COLUMN IF NOT EXISTS org_match_method TEXT    CHECK (org_match_method IN ('exact', 'alias', 'fuzzy', 'manual'));
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_orgs_meta_type      ON prosopography.organizations(meta_type);
CREATE INDEX IF NOT EXISTS idx_orgs_sector         ON prosopography.organizations(sector);
CREATE INDEX IF NOT EXISTS idx_orgs_location_country ON prosopography.organizations(location_country);
CREATE INDEX IF NOT EXISTS idx_orgs_un_tag         ON prosopography.organizations(un_canonical_tag);
CREATE INDEX IF NOT EXISTS idx_orgs_gov_tag        ON prosopography.organizations(gov_canonical_tag);
CREATE INDEX IF NOT EXISTS idx_aliases_org_id      ON prosopography.organization_aliases(org_id);
CREATE INDEX IF NOT EXISTS idx_cp_org_id           ON prosopography.career_positions(org_id);
"""


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    print("Creating organizations and organization_aliases tables...")
    cur.execute(DDL)

    print("Adding org_id and org_match_method to career_positions...")
    cur.execute(ALTER_CAREER_POSITIONS)

    print("Creating indexes...")
    for stmt in INDEXES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

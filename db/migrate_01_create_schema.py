"""
migrate_01_create_schema.py
-----------------------------
Creates the prosopography schema, all tables, and indexes.
Seeds the 4 HLP reference rows.
Safe to re-run (uses IF NOT EXISTS and ON CONFLICT DO NOTHING).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from db_utils import get_connection

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE SCHEMA IF NOT EXISTS prosopography;

-- ──────────────────────────────────────────────────────────────
-- Drop prototype tables from the v3 haphazard development phase.
-- These had 0-12 rows and no meaningful data to preserve.
-- Organization data (canonical_organizations, organization_aliases)
-- is also dropped; it will be rebuilt properly in Part 2.
-- ──────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS prosopography.user_corrections           CASCADE;
DROP TABLE IF EXISTS prosopography.verification_issues        CASCADE;
DROP TABLE IF EXISTS prosopography.issues_dashboard           CASCADE;
DROP TABLE IF EXISTS prosopography.source_evidence            CASCADE;
DROP TABLE IF EXISTS prosopography.events_complete            CASCADE;
DROP TABLE IF EXISTS prosopography.career_events              CASCADE;
DROP TABLE IF EXISTS prosopography.person_summary             CASCADE;
DROP TABLE IF EXISTS prosopography.persons                    CASCADE;
DROP TABLE IF EXISTS prosopography.evaluation_metrics         CASCADE;
DROP TABLE IF EXISTS prosopography.processing_decisions       CASCADE;
DROP TABLE IF EXISTS prosopography.sources_processed          CASCADE;
DROP TABLE IF EXISTS prosopography.organization_aliases       CASCADE;
DROP TABLE IF EXISTS prosopography.canonical_organizations    CASCADE;


-- HLP reference table (4 rows)
CREATE TABLE IF NOT EXISTS prosopography.hlp_panels (
    hlp_id          SERIAL PRIMARY KEY,
    hlp_name        TEXT     NOT NULL UNIQUE,
    hlp_year        SMALLINT NOT NULL,
    un_sg           TEXT,
    mandate_summary TEXT
);

-- Central persons table
CREATE TABLE IF NOT EXISTS prosopography.persons (
    person_id           SERIAL PRIMARY KEY,
    person_dir_name     TEXT     NOT NULL UNIQUE,
    display_name        TEXT     NOT NULL,
    sources_person_name TEXT     NOT NULL UNIQUE,
    birth_year          SMALLINT,
    death_status        TEXT     CHECK (death_status IN ('alive', 'deceased', 'unknown')),
    death_year          SMALLINT,
    hlp_id              INTEGER  NOT NULL REFERENCES prosopography.hlp_panels(hlp_id),
    hlp_nomination_age  SMALLINT,
    data_status         TEXT     NOT NULL DEFAULT 'base'
                                 CHECK (data_status IN ('base', 'derivative')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN prosopography.persons.hlp_nomination_age IS
    'Stored as-is from source data. Differs from hlp_year - birth_year for ~18 persons due to birthday timing within the year.';

COMMENT ON COLUMN prosopography.persons.sources_person_name IS
    'Matches sources.persons_searched.person_name. 10 persons have 2 rows in that table; use DISTINCT when joining.';

-- Multi-valued nationality
CREATE TABLE IF NOT EXISTS prosopography.person_nationalities (
    id          SERIAL PRIMARY KEY,
    person_id   INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    nationality TEXT     NOT NULL,
    sort_order  SMALLINT NOT NULL DEFAULT 0
);

-- Career positions (~2,183 rows)
CREATE TABLE IF NOT EXISTS prosopography.career_positions (
    position_id        SERIAL PRIMARY KEY,
    person_id          INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    title              TEXT     NOT NULL,
    organization       TEXT,
    time_start         SMALLINT,
    time_finish        SMALLINT,
    approximate_period TEXT,
    role_type          TEXT     CHECK (role_type IN ('primary', 'advisory', 'governance', 'other')),
    confidence         TEXT     CHECK (confidence IN ('high', 'medium', 'low')),
    event_source       TEXT     CHECK (event_source IN ('wikipedia', 'gap_finding')),
    source_count       SMALLINT,
    gap_source_url     TEXT,
    verified_sources   JSONB,
    supporting_quotes  JSONB,
    sort_order         SMALLINT NOT NULL DEFAULT 0,
    data_status        TEXT     NOT NULL DEFAULT 'base'
                                CHECK (data_status IN ('base', 'derivative')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN prosopography.career_positions.organization IS
    'Plain text in Part 1. An org_id FK to prosopography.organizations will be added in Part 2.';

-- Education (~219 rows)
-- Note: source JSON has no confidence field on education entries; that column is intentionally absent.
CREATE TABLE IF NOT EXISTS prosopography.education (
    education_id        SERIAL PRIMARY KEY,
    person_id           INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    degree_name         TEXT,
    degree_type         TEXT     CHECK (degree_type IN
                            ('undergraduate', 'masters', 'doctoral', 'postdoctoral',
                             'professional', 'certificate', 'other')),
    field               TEXT,
    institution         TEXT,
    institution_country TEXT,
    time_start          SMALLINT,
    time_finish         SMALLINT,
    event_source        TEXT     CHECK (event_source IN ('wikipedia', 'gap_finding')),
    source_count        SMALLINT,
    gap_source_url      TEXT,
    verified_sources    JSONB,
    supporting_quotes   JSONB,
    sort_order          SMALLINT NOT NULL DEFAULT 0,
    data_status         TEXT     NOT NULL DEFAULT 'base'
                                 CHECK (data_status IN ('base', 'derivative')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Awards (~715 rows)
CREATE TABLE IF NOT EXISTS prosopography.awards (
    award_id              SERIAL PRIMARY KEY,
    person_id             INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    award_name            TEXT     NOT NULL,
    awarding_organization TEXT,
    award_type            TEXT     CHECK (award_type IN
                              ('recognition', 'prize', 'honorary_degree',
                               'medal', 'fellowship', 'other')),
    time_start            SMALLINT,
    confidence            TEXT     CHECK (confidence IN ('high', 'medium', 'low')),
    event_source          TEXT     CHECK (event_source IN ('wikipedia', 'gap_finding')),
    source_count          SMALLINT,
    gap_source_url        TEXT,
    verified_sources      JSONB,
    supporting_quotes     JSONB,
    sort_order            SMALLINT NOT NULL DEFAULT 0,
    data_status           TEXT     NOT NULL DEFAULT 'base'
                                   CHECK (data_status IN ('base', 'derivative')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pipeline run metadata (one row per person per pipeline execution)
CREATE TABLE IF NOT EXISTS prosopography.pipeline_runs (
    run_id                 SERIAL PRIMARY KEY,
    person_id              INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    pipeline_name          TEXT     NOT NULL,
    generated_at           TIMESTAMPTZ NOT NULL,
    career_events_source   TEXT,
    status                 TEXT,
    events_used            SMALLINT,
    total_career_positions SMALLINT,
    total_education        SMALLINT,
    total_awards           SMALLINT,
    source_filename        TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (person_id, pipeline_name, generated_at)
);

-- Biographical provenance (one row per pipeline run per field per person)
CREATE TABLE IF NOT EXISTS prosopography.biographical_provenance (
    prov_id               SERIAL PRIMARY KEY,
    person_id             INTEGER  NOT NULL REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    question_id           TEXT     NOT NULL
                          CHECK (question_id IN ('birth_year', 'death_status', 'death_year', 'nationality')),
    run_timestamp         TIMESTAMPTZ NOT NULL,
    source_filename       TEXT     NOT NULL,
    service_name          TEXT,
    service_version       TEXT,
    model_used            TEXT,
    verified_answer       TEXT,
    verification_status   TEXT,
    source_count          SMALLINT,
    substantiation_status TEXT,
    retrieval_json        JSONB,
    extractions_json      JSONB,
    verification_json     JSONB,
    substantiation_json   JSONB,
    provenance_narrative  TEXT,
    referenced_chunk_ids  INTEGER[],
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (person_id, question_id, run_timestamp)
);

COMMENT ON COLUMN prosopography.biographical_provenance.referenced_chunk_ids IS
    'IDs from sources.chunks referenced during retrieval. Not enforced as FK (array); use = ANY(...) to join.';
"""

INDEXES = """
-- persons
CREATE INDEX IF NOT EXISTS idx_persons_hlp_id       ON prosopography.persons(hlp_id);
CREATE INDEX IF NOT EXISTS idx_persons_birth_year   ON prosopography.persons(birth_year);
CREATE INDEX IF NOT EXISTS idx_persons_death_status ON prosopography.persons(death_status);

-- person_nationalities
CREATE INDEX IF NOT EXISTS idx_nat_person_id   ON prosopography.person_nationalities(person_id);
CREATE INDEX IF NOT EXISTS idx_nat_nationality ON prosopography.person_nationalities(nationality);

-- career_positions
CREATE INDEX IF NOT EXISTS idx_cp_person_id    ON prosopography.career_positions(person_id);
CREATE INDEX IF NOT EXISTS idx_cp_time_start   ON prosopography.career_positions(time_start);
CREATE INDEX IF NOT EXISTS idx_cp_time_finish  ON prosopography.career_positions(time_finish);
CREATE INDEX IF NOT EXISTS idx_cp_organization ON prosopography.career_positions(organization);
CREATE INDEX IF NOT EXISTS idx_cp_confidence   ON prosopography.career_positions(confidence);
CREATE INDEX IF NOT EXISTS idx_cp_event_source ON prosopography.career_positions(event_source);

-- education
CREATE INDEX IF NOT EXISTS idx_edu_person_id   ON prosopography.education(person_id);
CREATE INDEX IF NOT EXISTS idx_edu_degree_type ON prosopography.education(degree_type);
CREATE INDEX IF NOT EXISTS idx_edu_institution ON prosopography.education(institution);

-- awards
CREATE INDEX IF NOT EXISTS idx_aw_person_id  ON prosopography.awards(person_id);
CREATE INDEX IF NOT EXISTS idx_aw_award_type ON prosopography.awards(award_type);
CREATE INDEX IF NOT EXISTS idx_aw_time_start ON prosopography.awards(time_start);

-- provenance
CREATE INDEX IF NOT EXISTS idx_bio_prov_person_id   ON prosopography.biographical_provenance(person_id);
CREATE INDEX IF NOT EXISTS idx_bio_prov_question_id ON prosopography.biographical_provenance(question_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_person ON prosopography.pipeline_runs(person_id);
"""

# GIN indexes require separate statements (IF NOT EXISTS not supported on GIN in older PG)
GIN_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_cp_verified_sources_gin ON prosopography.career_positions USING gin(verified_sources)",
    "CREATE INDEX IF NOT EXISTS idx_aw_verified_sources_gin ON prosopography.awards USING gin(verified_sources)",
]

HLP_SEED = [
    ("Threats, Challenges and Change", 2004, "Kofi Annan", None),
    ("System-Wide Coherence",          2007, "Kofi Annan", None),
    ("Post-2015 Development Agenda",   2012, "Ban Ki-moon", None),
    ("Digital Cooperation",            2020, "António Guterres", None),
]


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    print("Creating schema and tables...")
    cur.execute(DDL)

    print("Creating indexes...")
    for stmt in INDEXES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    for stmt in GIN_INDEXES:
        cur.execute(stmt)

    print("Seeding hlp_panels...")
    for hlp_name, hlp_year, un_sg, mandate in HLP_SEED:
        cur.execute(
            """
            INSERT INTO prosopography.hlp_panels (hlp_name, hlp_year, un_sg, mandate_summary)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (hlp_name) DO NOTHING
            """,
            (hlp_name, hlp_year, un_sg, mandate),
        )

    conn.commit()
    cur.close()
    conn.close()

    print("Done. Schema, tables, indexes, and HLP seed data created.")


if __name__ == "__main__":
    main()

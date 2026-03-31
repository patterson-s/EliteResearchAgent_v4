"""
migrate_09_create_derivatives_schema.py
----------------------------------------
Creates the three derivative tables in the prosopography schema:

  - derivative_runs   : one row per analytical pipeline run (provenance header)
  - position_tags     : per-career-position typed tags (FK -> career_positions)
  - person_attributes : flexible named attributes per person (FK -> persons)

Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
-- ── Derivative Runs (provenance header) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.derivative_runs (
    run_id              SERIAL PRIMARY KEY,
    run_name            TEXT        NOT NULL,
    derivative_type     TEXT        NOT NULL,
    entity_level        TEXT        NOT NULL
                        CHECK (entity_level IN ('position', 'person', 'organization')),
    model_used          TEXT,
    prompt_version      TEXT,
    prompt_text         TEXT,
    config_json         JSONB,
    scope_json          JSONB,
    narrative           TEXT,
    replication_notes   TEXT,
    evaluation_status   TEXT        NOT NULL DEFAULT 'draft'
                        CHECK (evaluation_status IN ('draft', 'reviewed', 'validated')),
    evaluation_notes    TEXT,
    n_processed         INTEGER,
    run_timestamp       TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Position Tags ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.position_tags (
    tag_id              SERIAL PRIMARY KEY,
    position_id         INTEGER     NOT NULL
                        REFERENCES prosopography.career_positions(position_id) ON DELETE CASCADE,
    run_id              INTEGER     NOT NULL
                        REFERENCES prosopography.derivative_runs(run_id),
    -- 8 typed dimensions from career_tags_v1
    domain              TEXT[],
    organization_type   TEXT,
    un_placement        TEXT,
    geographic_scope    TEXT,
    role_type           TEXT,
    function            TEXT,
    career_phase        TEXT,
    policy_bridge       BOOLEAN,
    -- overflow for future tagging rounds
    extra_tags          JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (position_id, run_id)
);

-- ── Person Attributes ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.person_attributes (
    attr_id             SERIAL PRIMARY KEY,
    person_id           INTEGER     NOT NULL
                        REFERENCES prosopography.persons(person_id) ON DELETE CASCADE,
    run_id              INTEGER     NOT NULL
                        REFERENCES prosopography.derivative_runs(run_id),
    attribute_name      TEXT        NOT NULL,
    attribute_value     TEXT        NOT NULL,
    attribute_label     TEXT,
    confidence          TEXT        CHECK (confidence IN ('high', 'medium', 'low')),
    is_primary          BOOLEAN     NOT NULL DEFAULT true,
    extra_data          JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (person_id, run_id, attribute_name)
);

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_deriv_runs_type
    ON prosopography.derivative_runs(derivative_type);

CREATE INDEX IF NOT EXISTS idx_pos_tags_position_id
    ON prosopography.position_tags(position_id);

CREATE INDEX IF NOT EXISTS idx_pos_tags_run_id
    ON prosopography.position_tags(run_id);

CREATE INDEX IF NOT EXISTS idx_pos_tags_career_phase
    ON prosopography.position_tags(career_phase);

CREATE INDEX IF NOT EXISTS idx_pos_tags_domain
    ON prosopography.position_tags USING GIN (domain);

CREATE INDEX IF NOT EXISTS idx_person_attrs_person_id
    ON prosopography.person_attributes(person_id);

CREATE INDEX IF NOT EXISTS idx_person_attrs_name_value
    ON prosopography.person_attributes(attribute_name, attribute_value);
"""


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        print("migrate_09: derivative schema created successfully.")

        # Report table existence
        for tbl in ("derivative_runs", "position_tags", "person_attributes"):
            cur.execute(
                "SELECT to_regclass('prosopography.%s') IS NOT NULL" % tbl
            )
            exists = cur.fetchone()[0]
            print(f"  prosopography.{tbl}: {'OK' if exists else 'MISSING'}")

        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

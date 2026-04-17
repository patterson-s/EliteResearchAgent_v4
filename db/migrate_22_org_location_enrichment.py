"""
migrate_22_org_location_enrichment.py
--------------------------------------
Adds location enrichment support to the organizations schema:

  1. Adds `location_region` TEXT column to prosopography.organizations
  2. Creates prosopography.org_location_searches provenance table
     (stores Serper results, Cohere rerank scores, extracted location,
      multi-source validation, and applied status per org per run)

Safe to re-run: uses ADD COLUMN IF NOT EXISTS + CREATE TABLE IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
-- ── Add location_region to organizations ─────────────────────────────────
ALTER TABLE prosopography.organizations
    ADD COLUMN IF NOT EXISTS location_region TEXT;

-- ── Org Location Searches (provenance table) ──────────────────────────────
CREATE TABLE IF NOT EXISTS prosopography.org_location_searches (
    search_id         SERIAL PRIMARY KEY,
    run_id            INTEGER     NOT NULL
                      REFERENCES prosopography.derivative_runs(run_id),
    org_id            INTEGER     NOT NULL
                      REFERENCES prosopography.organizations(org_id) ON DELETE CASCADE,
    search_query      TEXT,
    serper_results    JSONB,
    rerank_scores     JSONB,
    extracted_city    TEXT,
    extracted_country TEXT,
    extracted_region  TEXT,
    confidence        NUMERIC(4,3),
    sources_used      JSONB,
    sources_validated INTEGER     DEFAULT 0,
    applied           BOOLEAN     DEFAULT FALSE,
    created_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE (org_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_org_loc_searches_run_id
    ON prosopography.org_location_searches(run_id);

CREATE INDEX IF NOT EXISTS idx_org_loc_searches_org_id
    ON prosopography.org_location_searches(org_id);

CREATE INDEX IF NOT EXISTS idx_org_loc_searches_applied
    ON prosopography.org_location_searches(applied);
"""


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        print("migrate_22: org location enrichment schema applied.")

        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='prosopography' AND table_name='organizations' "
            "AND column_name='location_region'"
        )
        print(f"  location_region column: {'OK' if cur.fetchone() else 'MISSING'}")

        cur.execute(
            "SELECT to_regclass('prosopography.org_location_searches') IS NOT NULL"
        )
        print(f"  org_location_searches table: {'OK' if cur.fetchone()[0] else 'MISSING'}")

        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

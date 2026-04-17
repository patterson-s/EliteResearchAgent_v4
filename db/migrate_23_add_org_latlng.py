"""
migrate_23_add_org_latlng.py
----------------------------
Adds lat/lng coordinate columns to prosopography.organizations
for use in the Locations map view.

Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

DDL = """
ALTER TABLE prosopography.organizations
    ADD COLUMN IF NOT EXISTS location_lat  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS location_lng  DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_orgs_location_latlng
    ON prosopography.organizations(location_lat, location_lng)
    WHERE location_lat IS NOT NULL;
"""


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE location_lat IS NOT NULL) AS with_coords,
                COUNT(*) AS total
            FROM prosopography.organizations
        """)
        row = cur.fetchone()
        print(f"[migrate_23] Done. Orgs with coords: {row[0]}/{row[1]}")
        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

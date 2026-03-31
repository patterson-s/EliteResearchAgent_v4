"""
migrate_07_load_organizations.py
---------------------------------
Loads 1,609 organization entries from v3/services/ontology_01/final_ontology.json
into prosopography.organizations and prosopography.organization_aliases.

Idempotency: ON CONFLICT (canonical_name) DO UPDATE for organizations;
             ON CONFLICT (alias) DO NOTHING for aliases.
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

ONTOLOGY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "eliteresearchagent_v3",
    "services", "ontology_01", "final_ontology.json"
)

VALID_META_TYPES = {"io", "gov", "university", "ngo", "private", "other"}
VALID_REVIEW_STATUS = {"completed", "pending_review", "merged", "base"}


def parse_review_status(raw_status: str | None) -> str:
    """Map source status values to our CHECK constraint values."""
    if raw_status is None:
        return "base"         # Original high-quality entries have no status field
    if raw_status in VALID_REVIEW_STATUS:
        return raw_status
    return "pending_review"   # Unknown → pending review


def extract_string_array(value) -> list[str] | None:
    """Ensure value is a list of non-empty strings, or None."""
    if not value:
        return None
    if isinstance(value, list):
        cleaned = [str(v).strip() for v in value if v and str(v).strip()]
        return cleaned if cleaned else None
    return None


def main() -> None:
    with open(ONTOLOGY_PATH, encoding="utf-8") as f:
        data = json.load(f)
    orgs = data["final_ontology"]
    print(f"Loaded {len(orgs)} organizations from ontology JSON.")

    conn = get_connection()
    cur = conn.cursor()

    inserted_orgs = updated_orgs = inserted_aliases = skipped_aliases = 0

    for entry in orgs:
        canonical_name = entry.get("canonical_name", "").strip()
        if not canonical_name:
            continue

        meta_type = entry.get("meta_type")
        if meta_type not in VALID_META_TYPES:
            meta_type = "other"

        org_types = extract_string_array(entry.get("org_types"))
        sector = entry.get("sector") or None
        location_country = entry.get("location_country") or None
        location_city = entry.get("location_city") or None
        source = entry.get("source") or None
        parent_org_name = entry.get("parent_org") or None
        review_status = parse_review_status(entry.get("status"))

        # UN sub-ontology
        un = entry.get("un_ontology") or {}
        un_canonical_tag = un.get("canonical_tag") or None
        un_hierarchical_tags = extract_string_array(un.get("hierarchical_tags"))

        # GOV sub-ontology
        gov = entry.get("gov_ontology") or {}
        gov_canonical_tag = gov.get("canonical_tag") or None
        gov_hierarchical_tags = extract_string_array(gov.get("hierarchical_tags"))
        gov_country = gov.get("country") or None

        cur.execute(
            """
            INSERT INTO prosopography.organizations
                (canonical_name, meta_type, org_types, sector,
                 location_country, location_city,
                 un_canonical_tag, un_hierarchical_tags,
                 gov_canonical_tag, gov_hierarchical_tags, gov_country,
                 parent_org_name, source, review_status, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (canonical_name) DO UPDATE SET
                meta_type             = EXCLUDED.meta_type,
                org_types             = EXCLUDED.org_types,
                sector                = EXCLUDED.sector,
                location_country      = EXCLUDED.location_country,
                location_city         = EXCLUDED.location_city,
                un_canonical_tag      = EXCLUDED.un_canonical_tag,
                un_hierarchical_tags  = EXCLUDED.un_hierarchical_tags,
                gov_canonical_tag     = EXCLUDED.gov_canonical_tag,
                gov_hierarchical_tags = EXCLUDED.gov_hierarchical_tags,
                gov_country           = EXCLUDED.gov_country,
                parent_org_name       = EXCLUDED.parent_org_name,
                source                = EXCLUDED.source,
                review_status         = EXCLUDED.review_status,
                updated_at            = now()
            RETURNING (xmax = 0) AS is_insert
            """,
            (
                canonical_name, meta_type, org_types, sector,
                location_country, location_city,
                un_canonical_tag, un_hierarchical_tags,
                gov_canonical_tag, gov_hierarchical_tags, gov_country,
                parent_org_name, source, review_status,
            ),
        )
        row = cur.fetchone()
        if row and row[0]:
            inserted_orgs += 1
        else:
            updated_orgs += 1

        # Fetch org_id for alias insertion
        cur.execute(
            "SELECT org_id FROM prosopography.organizations WHERE canonical_name = %s",
            (canonical_name,),
        )
        org_id = cur.fetchone()[0]

        # Insert aliases from variations_found
        variations = entry.get("variations_found") or []
        for variation in variations:
            variation = variation.strip()
            if not variation:
                continue
            # Don't insert the canonical name itself as an alias
            if variation.lower() == canonical_name.lower():
                continue
            cur.execute(
                """
                INSERT INTO prosopography.organization_aliases (org_id, alias)
                VALUES (%s, %s)
                ON CONFLICT (alias) DO NOTHING
                """,
                (org_id, variation),
            )
            if cur.rowcount > 0:
                inserted_aliases += 1
            else:
                skipped_aliases += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nOrganizations: {inserted_orgs} inserted, {updated_orgs} updated.")
    print(f"Aliases: {inserted_aliases} inserted, {skipped_aliases} skipped (duplicate alias).")


if __name__ == "__main__":
    main()

"""
migrate_02_load_persons.py
---------------------------
Loads persons and person_nationalities from v3 base.json files.

Sources:
  - v3/services/targeted_01/data/[Name]/[Name]_base.json  (primary: HLP, demographics)
  - sources.persons_searched                               (for sources_person_name)

Idempotency: ON CONFLICT (person_dir_name) DO UPDATE for persons;
             DELETE + re-insert for nationalities.
"""

import sys
import os
import json
import difflib

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

V3_DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "eliteresearchagent_v3",
    "services", "targeted_01", "data"
)


def normalize_name(dir_name: str) -> str:
    """Convert filesystem dir name to display name.

    Abhijit_Banerjee      → Abhijit Banerjee
    Amina_J._Mohammed     → Amina J. Mohammed  (collapse double-space from dot)
    Graça_Machel          → Graça Machel       (Unicode preserved)
    """
    return " ".join(dir_name.replace("_", " ").split())


def build_sources_lookup(conn) -> dict[str, str]:
    """Return {normalized_display_name: original_sources_person_name} from DB."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT person_name FROM sources.persons_searched ORDER BY person_name")
    rows = cur.fetchall()
    cur.close()
    return {normalize_name(r[0].replace(" ", "_")): r[0] for r in rows}


def fuzzy_match(display_name: str, sources_lookup: dict[str, str]) -> str | None:
    """Return the best-matching sources_person_name, or None if no good match."""
    # Try exact match first
    normalized = normalize_name(display_name.replace(" ", "_"))
    if normalized in sources_lookup:
        return sources_lookup[normalized]

    # Fuzzy fallback using SequenceMatcher
    candidates = list(sources_lookup.keys())
    matches = difflib.get_close_matches(normalized, candidates, n=1, cutoff=0.6)
    if matches:
        return sources_lookup[matches[0]]
    return None


def load_base_json(person_dir_name: str) -> dict:
    path = os.path.join(V3_DATA_DIR, person_dir_name, f"{person_dir_name}_base.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # Build HLP name → hlp_id lookup
    cur.execute("SELECT hlp_id, hlp_name FROM prosopography.hlp_panels")
    hlp_map: dict[str, int] = {row[1]: row[0] for row in cur.fetchall()}

    # Build sources person name lookup
    sources_lookup = build_sources_lookup(conn)

    person_dirs = sorted(os.listdir(V3_DATA_DIR))
    print(f"Found {len(person_dirs)} person directories.")

    inserted = updated = skipped = 0
    unmatched_sources: list[str] = []

    for person_dir_name in person_dirs:
        base_path = os.path.join(V3_DATA_DIR, person_dir_name, f"{person_dir_name}_base.json")
        if not os.path.exists(base_path):
            print(f"  WARN: No base.json for {person_dir_name}, skipping.")
            skipped += 1
            continue

        data = load_base_json(person_dir_name)
        display_name = normalize_name(person_dir_name)

        # Resolve sources_person_name
        sources_person_name = fuzzy_match(display_name, sources_lookup)
        if sources_person_name is None:
            print(f"  WARN: No sources match for '{display_name}' — using display_name as fallback.")
            sources_person_name = display_name
            unmatched_sources.append(display_name)

        # Resolve HLP
        hlp_name = data.get("hlp_name", "").strip()
        hlp_id = hlp_map.get(hlp_name)
        if hlp_id is None:
            print(f"  ERROR: Unknown hlp_name '{hlp_name}' for {person_dir_name}. Skipping.")
            skipped += 1
            continue

        birth_year_raw = data.get("birth_year")
        birth_year = int(birth_year_raw) if birth_year_raw else None
        death_status = data.get("death_status") or "unknown"
        death_year_raw = data.get("death_year")
        death_year = int(death_year_raw) if death_year_raw else None
        hlp_nomination_age_raw = data.get("hlp_nomination_age")
        hlp_nomination_age = int(hlp_nomination_age_raw) if hlp_nomination_age_raw else None

        # Upsert person
        cur.execute(
            """
            INSERT INTO prosopography.persons
                (person_dir_name, display_name, sources_person_name,
                 birth_year, death_status, death_year,
                 hlp_id, hlp_nomination_age, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (person_dir_name) DO UPDATE SET
                display_name        = EXCLUDED.display_name,
                sources_person_name = EXCLUDED.sources_person_name,
                birth_year          = EXCLUDED.birth_year,
                death_status        = EXCLUDED.death_status,
                death_year          = EXCLUDED.death_year,
                hlp_id              = EXCLUDED.hlp_id,
                hlp_nomination_age  = EXCLUDED.hlp_nomination_age,
                updated_at          = now()
            RETURNING (xmax = 0) AS is_insert
            """,
            (person_dir_name, display_name, sources_person_name,
             birth_year, death_status, death_year,
             hlp_id, hlp_nomination_age),
        )
        row = cur.fetchone()
        if row and row[0]:
            inserted += 1
        else:
            updated += 1

        # Fetch person_id
        cur.execute(
            "SELECT person_id FROM prosopography.persons WHERE person_dir_name = %s",
            (person_dir_name,),
        )
        person_id = cur.fetchone()[0]

        # Replace nationalities
        cur.execute(
            "DELETE FROM prosopography.person_nationalities WHERE person_id = %s",
            (person_id,),
        )
        nationalities: list = data.get("nationality") or []
        for idx, nat in enumerate(nationalities):
            cur.execute(
                """
                INSERT INTO prosopography.person_nationalities (person_id, nationality, sort_order)
                VALUES (%s, %s, %s)
                """,
                (person_id, nat, idx),
            )

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nPersons: {inserted} inserted, {updated} updated, {skipped} skipped.")
    if unmatched_sources:
        print(f"WARNING: {len(unmatched_sources)} persons had no sources.persons_searched match:")
        for name in unmatched_sources:
            print(f"  - {name}")
    else:
        print("All persons matched to sources.persons_searched.")


if __name__ == "__main__":
    main()

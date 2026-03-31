"""
migrate_11_load_person_attributes.py
--------------------------------------
Loads person-level derivative attributes from three v3 sources:

  Run 1 — career_domain_v1
    Source: v3/services/targeted_01/outputs/{Name}/{Name}_career_domain.json
    Attributes: career_domain, is_hybrid_domain

  Run 2 — geo_profile_v1
    Source: v3/analysis/locations/outputs/profiles/{Name}.json
    Attributes: mobility_pattern, institution_prestige, geo_edu_category

  Run 3 — ideal_types_v1  (loaded if person_map.json has per-person assignments)
    Source: v3/analysis/ideal_types/outputs/person_map.json
    Attributes: career_typology (primary + secondary)

Re-running is safe: uses ON CONFLICT (person_id, run_id, attribute_name) DO NOTHING.
"""

import sys
import os
import json
from pathlib import Path

import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

# ── Paths ─────────────────────────────────────────────────────────────────
V3_ROOT     = Path(r"C:\Users\spatt\Desktop\eliteresearchagent_v3")
TARGETED_01 = V3_ROOT / "services" / "targeted_01" / "outputs"
LOCATIONS   = V3_ROOT / "analysis" / "locations" / "outputs" / "profiles"
TYPOLOGY    = V3_ROOT / "analysis" / "typology" / "outputs" / "profiles"
IDEAL_TYPES = V3_ROOT / "analysis" / "ideal_types" / "outputs" / "person_map.json"

# ── Run metadata ───────────────────────────────────────────────────────────
RUNS = {
    "career_domain_v1": {
        "run_name":         "career_domain_v1",
        "derivative_type":  "career_domain",
        "entity_level":     "person",
        "model_used":       "claude-sonnet-4-6",
        "prompt_version":   "v1",
        "narrative": (
            "Dominant career domain classification for all 75 HLP members. "
            "Each person was classified into one of 7 domains (academic, political, "
            "diplomatic, development, corporate, civil_society, international) based on "
            "their full career history, education, sector engagement, and HLP appointment. "
            "Hybrid classification was flagged separately. Generated via the targeted_01 "
            "synthesis pipeline in v3."
        ),
        "replication_notes": (
            "Original pipeline: eliteresearchagent_v3/services/targeted_01/. "
            "Re-run the career_domain synthesis prompt against updated career data to "
            "produce a new career_domain_v2 run."
        ),
        "evaluation_status": "draft",
        "scope_json":       {"hlp_panels": [1, 2, 3, 4], "n_persons": 75, "filter": "all"},
        "run_timestamp":    "2026-02-25T17:08:11+00:00",  # from sample file
    },
    "geo_profile_v1": {
        "run_name":         "geo_profile_v1",
        "derivative_type":  "geo_trajectory",
        "entity_level":     "person",
        "model_used":       "claude-sonnet-4-6",
        "prompt_version":   "v1",
        "narrative": (
            "Geographic career profile attributes derived from the locations analysis "
            "pipeline in v3. Captures three person-level dimensions: mobility_pattern "
            "(level of geographic career mobility), institution_prestige (elite/non_elite/"
            "both based on institutions attended and worked at), and geo_edu_category "
            "(whether education was north-only, south-only, or both hemispheres)."
        ),
        "replication_notes": (
            "Original pipeline: eliteresearchagent_v3/analysis/locations/. "
            "Source: locations/outputs/profiles/{Name}.json, field: meta."
        ),
        "evaluation_status": "draft",
        "scope_json":       {"hlp_panels": [1, 2, 3, 4], "n_persons": 75, "filter": "all"},
        "run_timestamp":    "2026-02-01T00:00:00+00:00",
    },
    "ideal_types_v1": {
        "run_name":         "ideal_types_v1",
        "derivative_type":  "person_typology",
        "entity_level":     "person",
        "model_used":       "claude-sonnet-4-6",
        "prompt_version":   "v1",
        "narrative": (
            "Ideal-type career typology assignment for HLP members using a 7-type framework: "
            "DOMESTIC_POLITICAL_ELDER, CAREER_FOREIGN_SERVICE, DEVELOPMENT_CIRCUIT_RIDER, "
            "NATIONAL_TO_GLOBAL_PIVOT, DOMAIN_KNOWLEDGE_AUTHORITY, "
            "CIVIL_SOCIETY_PLATFORM_BUILDER, CORPORATE_TO_GOVERNANCE_CROSSOVER. "
            "Each person receives a primary type (highest scoring), secondary type, "
            "gap ratio, and confidence. Type definitions are stored in "
            "ideal_types/outputs/ideal_type_definitions.json."
        ),
        "replication_notes": (
            "Original pipeline: eliteresearchagent_v3/analysis/ideal_types/. "
            "Source: ideal_types/outputs/person_map.json."
        ),
        "evaluation_status": "draft",
        "scope_json":       {"hlp_panels": [1, 2, 3, 4], "n_persons": 75, "filter": "all"},
        "run_timestamp":    "2026-02-15T00:00:00+00:00",
    },
}


def get_or_create_run(cur, run_key: str) -> int:
    meta = RUNS[run_key]
    cur.execute(
        "SELECT run_id FROM prosopography.derivative_runs WHERE run_name = %s",
        (meta["run_name"],)
    )
    row = cur.fetchone()
    if row:
        print(f"  run '{meta['run_name']}' already exists (run_id={row[0]})")
        return row[0]
    cur.execute("""
        INSERT INTO prosopography.derivative_runs
            (run_name, derivative_type, entity_level, model_used, prompt_version,
             narrative, replication_notes, evaluation_status, scope_json, run_timestamp)
        VALUES (%(run_name)s, %(derivative_type)s, %(entity_level)s, %(model_used)s,
                %(prompt_version)s, %(narrative)s, %(replication_notes)s,
                %(evaluation_status)s, %(scope_json)s, %(run_timestamp)s)
        RETURNING run_id
    """, {**meta, "scope_json": json.dumps(meta["scope_json"])})
    run_id = cur.fetchone()[0]
    print(f"  created run '{meta['run_name']}' run_id={run_id}")
    return run_id


def load_person_id_map(conn) -> dict[str, int]:
    """Returns {person_dir_name: person_id}."""
    cur = conn.cursor()
    cur.execute("SELECT person_dir_name, person_id FROM prosopography.persons")
    result = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return result


def bulk_insert_attrs(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO prosopography.person_attributes
            (person_id, run_id, attribute_name, attribute_value,
             attribute_label, confidence, is_primary, extra_data)
        VALUES %s
        ON CONFLICT (person_id, run_id, attribute_name) DO NOTHING
        """,
        [(
            r["person_id"], r["run_id"], r["attribute_name"], r["attribute_value"],
            r.get("attribute_label"), r.get("confidence"),
            r.get("is_primary", True),
            json.dumps(r["extra_data"]) if r.get("extra_data") else None,
        ) for r in rows],
        page_size=200,
    )
    return cur.rowcount if cur.rowcount != -1 else len(rows)


# ── Run 1: career_domain ──────────────────────────────────────────────────

def load_career_domain(conn, person_map: dict[str, int]) -> None:
    print("\n── Run 1: career_domain_v1 ──")
    cur = conn.cursor()
    run_id = get_or_create_run(cur, "career_domain_v1")
    conn.commit()

    rows: list[dict] = []
    skipped = 0

    for person_dir in sorted(person_map.keys()):
        person_id = person_map[person_dir]
        dominant = is_hybrid = hybrid_domains = domain_evidence = confidence = None

        # Primary source: targeted_01 (detailed provenance)
        t1_path = TARGETED_01 / person_dir / f"{person_dir}_career_domain.json"
        if t1_path.exists():
            with open(t1_path, encoding="utf-8") as f:
                data = json.load(f)
            result = data.get("result", {})
            parsed = data.get("parsed", {})
            dominant = result.get("dominant_domain") or parsed.get("dominant_domain")
            is_hybrid = result.get("is_hybrid", False)
            hybrid_domains = result.get("hybrid_domains", []) or []
            confidence_raw = result.get("confidence", "").lower()
            confidence = confidence_raw if confidence_raw in ("high", "medium", "low") else None
            domain_evidence = result.get("domain_evidence", {})

        # Fallback: typology profile meta (covers all 75 persons)
        if not dominant:
            typ_path = TYPOLOGY / f"{person_dir}.json"
            if typ_path.exists():
                with open(typ_path, encoding="utf-8") as f:
                    data = json.load(f)
                meta = data.get("meta", {})
                dominant = meta.get("dominant_domain")
                is_hybrid = meta.get("is_hybrid", False)
                hybrid_domains = []
                domain_evidence = {}
            else:
                skipped += 1
                continue

        if not dominant:
            skipped += 1
            continue

        # career_domain attribute
        rows.append({
            "person_id":      person_id,
            "run_id":         run_id,
            "attribute_name": "career_domain",
            "attribute_value": dominant,
            "confidence":     confidence,
            "is_primary":     True,
            "extra_data": {
                "hybrid_domains":   hybrid_domains or [],
                "domain_evidence":  domain_evidence or {},
                "source":           "targeted_01" if t1_path.exists() else "typology_profile",
            },
        })

        # is_hybrid_domain attribute
        rows.append({
            "person_id":      person_id,
            "run_id":         run_id,
            "attribute_name": "is_hybrid_domain",
            "attribute_value": str(bool(is_hybrid)).lower(),
            "confidence":     confidence,
            "is_primary":     True,
            "extra_data":     {"hybrid_domains": hybrid_domains or []},
        })

    inserted = bulk_insert_attrs(cur, rows)
    conn.commit()

    cur.execute(
        "UPDATE prosopography.derivative_runs SET n_processed = %s WHERE run_id = %s",
        (len(rows) // 2, run_id)
    )
    conn.commit()
    cur.close()
    print(f"  Persons with data : {len(rows) // 2}  |  skipped: {skipped}")
    print(f"  Rows inserted     : {inserted}")


# ── Run 2: geo_profile ────────────────────────────────────────────────────

def load_geo_profile(conn, person_map: dict[str, int]) -> None:
    print("\n── Run 2: geo_profile_v1 ──")
    cur = conn.cursor()
    run_id = get_or_create_run(cur, "geo_profile_v1")
    conn.commit()

    rows: list[dict] = []
    skipped = 0

    for person_dir in sorted(person_map.keys()):
        person_id = person_map[person_dir]
        meta = {}

        # Primary source: locations profile
        loc_path = LOCATIONS / f"{person_dir}.json"
        if loc_path.exists():
            with open(loc_path, encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("meta", {})

        # Fallback: typology profile (has same meta fields)
        if not any(meta.get(k) for k in ("mobility_pattern", "institution_prestige", "geo_edu_category")):
            typ_path = TYPOLOGY / f"{person_dir}.json"
            if typ_path.exists():
                with open(typ_path, encoding="utf-8") as f:
                    data = json.load(f)
                meta = data.get("meta", {})

        if not meta:
            skipped += 1
            continue

        for attr_name, attr_val in [
            ("mobility_pattern",     meta.get("mobility_pattern")),
            ("institution_prestige", meta.get("institution_prestige")),
            ("geo_edu_category",     meta.get("geo_edu_category")),
        ]:
            if attr_val:
                rows.append({
                    "person_id":      person_id,
                    "run_id":         run_id,
                    "attribute_name": attr_name,
                    "attribute_value": attr_val,
                    "is_primary":     True,
                })

    inserted = bulk_insert_attrs(cur, rows)
    conn.commit()

    persons_loaded = len(rows) // 3 if rows else 0
    cur.execute(
        "UPDATE prosopography.derivative_runs SET n_processed = %s WHERE run_id = %s",
        (persons_loaded, run_id)
    )
    conn.commit()
    cur.close()
    print(f"  Persons with data : {persons_loaded}  |  skipped: {skipped}")
    print(f"  Rows inserted     : {inserted}")


# ── Run 3: ideal_types ────────────────────────────────────────────────────

def load_ideal_types(conn, person_map: dict[str, int]) -> None:
    print("\n── Run 3: ideal_types_v1 ──")

    if not IDEAL_TYPES.exists():
        print("  person_map.json not found — skipping.")
        return

    with open(IDEAL_TYPES, encoding="utf-8") as f:
        data = json.load(f)

    # Check that it has per-person assignments (dict keyed by person_dir)
    sample_val = next(iter(data.values()), None) if isinstance(data, dict) else None
    if not isinstance(sample_val, dict) or "primary_type" not in sample_val:
        print("  person_map.json does not contain per-person type assignments — skipping.")
        return

    cur = conn.cursor()
    run_id = get_or_create_run(cur, "ideal_types_v1")
    conn.commit()

    rows: list[dict] = []
    skipped = 0

    for person_dir, entry in data.items():
        person_id = person_map.get(person_dir)
        if person_id is None:
            skipped += 1
            continue

        primary_type = entry.get("primary_type")
        if not primary_type:
            skipped += 1
            continue

        confidence_raw = entry.get("confidence", "").lower()
        confidence = confidence_raw if confidence_raw in ("high", "medium", "low") else None

        # Primary type
        rows.append({
            "person_id":      person_id,
            "run_id":         run_id,
            "attribute_name": "career_typology",
            "attribute_value": primary_type,
            "confidence":     confidence,
            "is_primary":     True,
            "extra_data": {
                "primary_score":   entry.get("primary_score"),
                "secondary_type":  entry.get("secondary_type"),
                "secondary_score": entry.get("secondary_score"),
                "gap_ratio":       entry.get("gap_ratio"),
                "between_types":   entry.get("between_types", False),
                "all_scores":      entry.get("all_scores", {}),
            },
        })

    inserted = bulk_insert_attrs(cur, rows)
    conn.commit()

    cur.execute(
        "UPDATE prosopography.derivative_runs SET n_processed = %s WHERE run_id = %s",
        (len(rows), run_id)
    )
    conn.commit()
    cur.close()
    print(f"  Persons typed     : {len(rows)}  |  skipped: {skipped}")
    print(f"  Rows inserted     : {inserted}")


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    conn = get_connection()
    try:
        person_map = load_person_id_map(conn)
        print(f"migrate_11: loaded {len(person_map)} persons from DB")

        load_career_domain(conn, person_map)
        load_geo_profile(conn, person_map)
        load_ideal_types(conn, person_map)

        # ── Summary ──────────────────────────────────────────────────────
        cur = conn.cursor()
        cur.execute("""
            SELECT attribute_name, COUNT(*) AS n
            FROM prosopography.person_attributes
            GROUP BY attribute_name
            ORDER BY attribute_name
        """)
        print("\nmigrate_11 summary — person_attributes table:")
        for row in cur.fetchall():
            print(f"  {row[0]:<30} {row[1]} rows")
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

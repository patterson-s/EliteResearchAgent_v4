"""
migrate_04_load_biographical_provenance.py
-------------------------------------------
Loads biographical_provenance from v3 per-field provenance JSON files.

Source pattern: v3/services/biographical/review/[Name]_[field]_[timestamp].json
Fields: birth_year, death_status, death_year, nationality

Idempotency: ON CONFLICT (person_id, question_id, run_timestamp) DO NOTHING.
             Amre_Moussa has 2 runs per field — both are inserted (different timestamps).
"""

import sys
import os
import json
import re
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

V3_BIO_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "eliteresearchagent_v3",
    "services", "biographical", "review"
)

VALID_QUESTION_IDS = {"birth_year", "death_status", "death_year", "nationality"}

# Filename pattern: [Name]_[question_id]_[YYYYMMDD]_[HHMMSS].json
# e.g. Abhijit_Banerjee_birth_year_20260212_163031.json
_PROV_PATTERN = re.compile(
    r"^(.+?)_(birth_year|death_status|death_year|nationality)_(\d{8}_\d{6})\.json$"
)


def extract_chunk_ids(data: dict) -> list[int]:
    """Collect all chunk_id values from retrieval and extractions blocks."""
    chunk_ids: set[int] = set()

    retrieval = data.get("retrieval") or {}
    for cand in retrieval.get("top_candidates") or []:
        cid = cand.get("chunk_id")
        if cid is not None:
            chunk_ids.add(int(cid))

    for extraction in data.get("extractions") or []:
        cid = extraction.get("chunk_id")
        if cid is not None:
            chunk_ids.add(int(cid))

    sub = data.get("substantiation") or {}
    cid = sub.get("chunk_id")
    if cid is not None:
        chunk_ids.add(int(cid))

    return sorted(chunk_ids)


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # Build display_name → person_id lookup (case-insensitive for safety)
    cur.execute("SELECT person_id, display_name, person_dir_name FROM prosopography.persons")
    persons = cur.fetchall()
    name_to_id: dict[str, int] = {}
    dir_to_id: dict[str, int] = {}
    for person_id, display_name, person_dir_name in persons:
        name_to_id[display_name.lower()] = person_id
        dir_to_id[person_dir_name] = person_id

    print(f"Loaded {len(persons)} persons.")

    prov_files = [f for f in os.listdir(V3_BIO_DIR) if _PROV_PATTERN.match(f)]
    print(f"Found {len(prov_files)} provenance files.")

    inserted = skipped_conflict = skipped_no_person = skipped_bad = 0

    for filename in sorted(prov_files):
        m = _PROV_PATTERN.match(filename)
        if not m:
            continue

        name_part, question_id, ts_str = m.group(1), m.group(2), m.group(3)

        if question_id not in VALID_QUESTION_IDS:
            skipped_bad += 1
            continue

        # Resolve person_id from the name part (underscored)
        person_id = dir_to_id.get(name_part)
        if person_id is None:
            # Try via display name
            display = " ".join(name_part.replace("_", " ").split()).lower()
            person_id = name_to_id.get(display)
        if person_id is None:
            print(f"  WARN: Cannot resolve person for '{name_part}' in {filename}. Skipping.")
            skipped_no_person += 1
            continue

        filepath = os.path.join(V3_BIO_DIR, filename)
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)

        # Parse run_timestamp from the JSON (authoritative), fall back to filename
        ts_raw = data.get("timestamp")
        if ts_raw:
            try:
                run_timestamp = datetime.fromisoformat(ts_raw)
                if run_timestamp.tzinfo is None:
                    run_timestamp = run_timestamp.replace(tzinfo=timezone.utc)
            except ValueError:
                run_timestamp = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        else:
            run_timestamp = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)

        # Config block
        config = data.get("config") or {}
        service_name    = config.get("service_name")
        service_version = config.get("version")
        model_used      = config.get("model")

        # Result block
        result = data.get("result") or {}
        verified_answer       = str(result.get("verified_answer")) if result.get("verified_answer") is not None else None
        verification_status   = result.get("verification_status")
        source_count_raw      = result.get("source_count")
        source_count          = int(source_count_raw) if source_count_raw is not None else None
        substantiation_status = result.get("substantiation_status")

        # Chunk IDs
        referenced_chunk_ids = extract_chunk_ids(data)

        # Phase JSONs (store as JSONB; these are audit data)
        retrieval_json      = json.dumps(data.get("retrieval"))      if data.get("retrieval")      else None
        extractions_json    = json.dumps(data.get("extractions"))    if data.get("extractions")    else None
        verification_json   = json.dumps(data.get("verification"))   if data.get("verification")   else None
        substantiation_json = json.dumps(data.get("substantiation")) if data.get("substantiation") else None
        provenance_narrative = data.get("provenance_narrative")

        try:
            cur.execute(
                """
                INSERT INTO prosopography.biographical_provenance
                    (person_id, question_id, run_timestamp, source_filename,
                     service_name, service_version, model_used,
                     verified_answer, verification_status, source_count, substantiation_status,
                     retrieval_json, extractions_json, verification_json, substantiation_json,
                     provenance_narrative, referenced_chunk_ids)
                VALUES
                    (%s, %s, %s, %s,
                     %s, %s, %s,
                     %s, %s, %s, %s,
                     %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                     %s, %s)
                ON CONFLICT (person_id, question_id, run_timestamp) DO NOTHING
                """,
                (
                    person_id, question_id, run_timestamp, filename,
                    service_name, service_version, model_used,
                    verified_answer, verification_status, source_count, substantiation_status,
                    retrieval_json, extractions_json, verification_json, substantiation_json,
                    provenance_narrative,
                    referenced_chunk_ids if referenced_chunk_ids else None,
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped_conflict += 1
        except Exception as e:
            print(f"  ERROR inserting {filename}: {e}")
            conn.rollback()
            skipped_bad += 1
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nBiographical provenance: {inserted} inserted, "
          f"{skipped_conflict} skipped (already exists), "
          f"{skipped_no_person} skipped (no person match), "
          f"{skipped_bad} skipped (bad data).")


if __name__ == "__main__":
    main()

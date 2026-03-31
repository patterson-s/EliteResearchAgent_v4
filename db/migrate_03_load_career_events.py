"""
migrate_03_load_career_events.py
---------------------------------
Loads career_positions, education, awards, and pipeline_runs from
v3/services/integrated_01/outputs/[Name]/[Name]_career_history.json.

Idempotency: Deletes all child rows for the person before re-inserting.
             pipeline_runs uses ON CONFLICT DO NOTHING.
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

V3_OUTPUTS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "eliteresearchagent_v3",
    "services", "integrated_01", "outputs"
)

# Known valid values for CHECK constraints; anything else gets remapped to 'other'
VALID_ROLE_TYPES = {"primary", "advisory", "governance", "other"}
VALID_AWARD_TYPES = {"recognition", "prize", "honorary_degree", "medal", "fellowship", "other"}
VALID_DEGREE_TYPES = {"undergraduate", "masters", "doctoral", "postdoctoral", "professional", "certificate", "other"}
VALID_CONFIDENCE = {"high", "medium", "low"}
VALID_EVENT_SOURCE = {"wikipedia", "gap_finding"}


def normalize_em_dash(text: str | None) -> str | None:
    """Normalize any em-dash variants to the standard en-dash U+2013."""
    if text is None:
        return None
    # Handle double-encoded artifact: â€" → –
    try:
        fixed = text.encode("latin-1").decode("utf-8")
        return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def clean_str(val) -> str | None:
    return str(val).strip() if val else None


def safe_smallint(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def extract_sources(sources: dict) -> tuple:
    """Extract provenance fields from a career event sources block."""
    event_source = sources.get("event_source")
    if event_source not in VALID_EVENT_SOURCE:
        event_source = None
    source_count = safe_smallint(sources.get("source_count"))
    gap_source_url = clean_str(sources.get("gap_source_url"))
    verified_sources = sources.get("verified_sources") or []
    supporting_quotes = sources.get("supporting_quotes") or []
    return event_source, source_count, gap_source_url, verified_sources, supporting_quotes


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    # Build person_dir_name → person_id lookup
    cur.execute("SELECT person_id, person_dir_name FROM prosopography.persons")
    person_map: dict[str, int] = {row[1]: row[0] for row in cur.fetchall()}
    print(f"Loaded {len(person_map)} persons from DB.")

    processed = skipped = 0
    total_positions = total_education = total_awards = 0

    for person_dir_name, person_id in sorted(person_map.items()):
        career_file = os.path.join(
            V3_OUTPUTS_DIR, person_dir_name, f"{person_dir_name}_career_history.json"
        )
        if not os.path.exists(career_file):
            print(f"  WARN: No career_history.json for {person_dir_name}, skipping.")
            skipped += 1
            continue

        with open(career_file, encoding="utf-8") as f:
            data = json.load(f)

        # Clear existing child rows for idempotency
        cur.execute("DELETE FROM prosopography.career_positions WHERE person_id = %s", (person_id,))
        cur.execute("DELETE FROM prosopography.education WHERE person_id = %s", (person_id,))
        cur.execute("DELETE FROM prosopography.awards WHERE person_id = %s", (person_id,))

        # ── career_positions ──────────────────────────────────────────────
        positions = data.get("career_positions") or []
        for idx, ev in enumerate(positions):
            sources = ev.get("sources") or {}
            event_source, source_count, gap_source_url, verified_sources, supporting_quotes = extract_sources(sources)

            role_type = ev.get("role_type")
            if role_type not in VALID_ROLE_TYPES:
                role_type = "other"

            confidence = ev.get("confidence")
            if confidence not in VALID_CONFIDENCE:
                confidence = None

            cur.execute(
                """
                INSERT INTO prosopography.career_positions
                    (person_id, title, organization, time_start, time_finish,
                     approximate_period, role_type, confidence, event_source,
                     source_count, gap_source_url, verified_sources, supporting_quotes,
                     sort_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    person_id,
                    clean_str(ev.get("title")),
                    clean_str(ev.get("organization")),
                    safe_smallint(ev.get("time_start")),
                    safe_smallint(ev.get("time_finish")),
                    normalize_em_dash(ev.get("approximate_period")),
                    role_type,
                    confidence,
                    event_source,
                    source_count,
                    gap_source_url,
                    json.dumps(verified_sources),
                    json.dumps(supporting_quotes),
                    idx,
                ),
            )
        total_positions += len(positions)

        # ── education ─────────────────────────────────────────────────────
        education = data.get("education") or []
        for idx, ev in enumerate(education):
            sources = ev.get("sources") or {}
            event_source, source_count, gap_source_url, verified_sources, supporting_quotes = extract_sources(sources)

            degree_type = ev.get("degree_type")
            if degree_type not in VALID_DEGREE_TYPES:
                degree_type = "other"

            cur.execute(
                """
                INSERT INTO prosopography.education
                    (person_id, degree_name, degree_type, field, institution,
                     institution_country, time_start, time_finish, event_source,
                     source_count, gap_source_url, verified_sources, supporting_quotes,
                     sort_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    person_id,
                    clean_str(ev.get("degree_name")),
                    degree_type,
                    clean_str(ev.get("field")),
                    clean_str(ev.get("institution")),
                    clean_str(ev.get("institution_country")),
                    safe_smallint(ev.get("time_start")),
                    safe_smallint(ev.get("time_finish")),
                    event_source,
                    source_count,
                    gap_source_url,
                    json.dumps(verified_sources),
                    json.dumps(supporting_quotes),
                    idx,
                ),
            )
        total_education += len(education)

        # ── awards ────────────────────────────────────────────────────────
        awards = data.get("awards") or []
        for idx, ev in enumerate(awards):
            sources = ev.get("sources") or {}
            event_source, source_count, gap_source_url, verified_sources, supporting_quotes = extract_sources(sources)

            award_type = ev.get("award_type")
            if award_type not in VALID_AWARD_TYPES:
                award_type = "other"

            confidence = ev.get("confidence")
            if confidence not in VALID_CONFIDENCE:
                confidence = None

            cur.execute(
                """
                INSERT INTO prosopography.awards
                    (person_id, award_name, awarding_organization, award_type, time_start,
                     confidence, event_source, source_count, gap_source_url,
                     verified_sources, supporting_quotes, sort_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    person_id,
                    clean_str(ev.get("award_name")),
                    clean_str(ev.get("awarding_organization")),
                    award_type,
                    safe_smallint(ev.get("time_start")),
                    confidence,
                    event_source,
                    source_count,
                    gap_source_url,
                    json.dumps(verified_sources),
                    json.dumps(supporting_quotes),
                    idx,
                ),
            )
        total_awards += len(awards)

        # ── pipeline_runs ─────────────────────────────────────────────────
        totals = data.get("totals") or {}
        cur.execute(
            """
            INSERT INTO prosopography.pipeline_runs
                (person_id, pipeline_name, generated_at, career_events_source,
                 status, events_used, total_career_positions, total_education, total_awards,
                 source_filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (person_id, pipeline_name, generated_at) DO NOTHING
            """,
            (
                person_id,
                clean_str(data.get("pipeline")),
                data.get("generated_at"),
                clean_str(data.get("career_events_source")),
                clean_str(data.get("status")),
                safe_smallint(data.get("events_used")),
                safe_smallint(totals.get("career_positions")),
                safe_smallint(totals.get("education")),
                safe_smallint(totals.get("awards")),
                os.path.basename(career_file),
            ),
        )

        processed += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nProcessed {processed} persons, skipped {skipped}.")
    print(f"Inserted: {total_positions} career_positions, {total_education} education, {total_awards} awards.")


if __name__ == "__main__":
    main()

"""
derive_functional_summary.py
------------------------------
Two-pass functional mobility analysis for all 75 HLP corpus members.

PASS 1 — Individual Assessment (75 API calls)
  Describes each person's career functions on their own terms — extracts
  primary_functions, domain_trajectory, key_transitions, and a factual arc.
  Intentionally does NOT assign mobility labels.
  Stored as: attribute_name='functional_profile'

PASS 2 — Comparative Calibration (1 API call)
  Presents all 75 functional profiles simultaneously and assigns calibrated
  mobility_type labels relative to the full corpus. An academic who founded
  a research institute is low-mobility relative to someone who moved from
  teacher to energy minister — this pass captures that relative calibration.
  Stored as: attribute_name='functional_summary'

Both use the same run_id. Both are idempotent via:
  ON CONFLICT (person_id, run_id, attribute_name) DO NOTHING
"""

import sys
import os
import json
import time
from datetime import datetime, timezone

# Force UTF-8 output on Windows consoles that default to cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg2
import psycopg2.extras
import anthropic
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

# Load .env from project root, overriding any stale system env vars
_env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(_env_path, override=True)

# ── Config ────────────────────────────────────────────────────────────────────

MODEL = "claude-sonnet-4-6"

RUN_META = {
    "run_name":         "functional_summary_v1",
    "derivative_type":  "functional_summary",
    "entity_level":     "person",
    "model_used":       MODEL,
    "prompt_version":   "v1",
    "narrative": (
        "Two-pass functional mobility analysis for all 75 HLP members. "
        "Pass 1: individual career function extraction (descriptive, no mobility labels). "
        "Pass 2: comparative calibration across the full corpus to assign relative "
        "mobility_type labels (monofunctional, bifunctional, multifunctional, transitional). "
        "Function is defined as the intersection of role type and substantive domain."
    ),
    "replication_notes": (
        "Run db/derive_functional_summary.py against updated career_positions + position_tags. "
        "Increment version in scope_json for a new run. "
        "Pass 1 is resumable; pass 2 re-runs if any person lacks functional_summary."
    ),
    "evaluation_status": "draft",
    "scope_json":       {"hlp_panels": [1, 2, 3, 4], "n_persons": 75, "version": 1},
}

MOBILITY_LABELS = {
    "monofunctional":  "Monofunctional",
    "bifunctional":    "Bifunctional",
    "multifunctional": "Multifunctional",
    "transitional":    "Transitional",
}

# ── Pass 1 Tool ───────────────────────────────────────────────────────────────

PASS1_TOOL = {
    "name": "record_functional_profile",
    "description": (
        "Record a factual description of this person's career functions. "
        "This is purely descriptive — do not classify mobility or use mobility labels."
    ),
    "input_schema": {
        "type": "object",
        "required": ["primary_functions", "domain_trajectory",
                     "raw_transition_count", "functional_arc"],
        "properties": {
            "primary_functions": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "1-5 composite role×domain labels capturing what the person did "
                    "and in what substantive area. Examples: 'academic_economics', "
                    "'health_practitioner', 'health_politician', "
                    "'environmental_governance', 'international_development', "
                    "'journalism', 'domestic_politics_foreign_policy'. "
                    "Each label should encode BOTH role type AND substantive domain."
                ),
            },
            "domain_trajectory": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Ordered list of substantive domains engaged across the career arc, "
                    "from earliest to latest. E.g. ['medicine', 'health_policy', "
                    "'environmental_policy']. List each domain only once."
                ),
            },
            "raw_transition_count": {
                "type": "integer",
                "description": (
                    "Count of genuine role×domain transitions in the career. "
                    "A politician who also published op-eds = 0 transitions. "
                    "A journalist who became a minister = 1 transition. "
                    "A doctor who became a health minister who became an "
                    "environmental politician = 2 transitions."
                ),
            },
            "functional_arc": {
                "type": "string",
                "description": (
                    "2-3 sentence factual narrative of how the person's functions "
                    "evolved. Name specific role types and substantive domains. "
                    "DO NOT use mobility classification terms. "
                    "DO NOT say 'monofunctional', 'high mobility', etc."
                ),
            },
            "key_transitions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "from":             {"type": "string"},
                        "to":               {"type": "string"},
                        "approximate_year": {"type": "integer"},
                        "note":             {"type": "string"},
                    },
                },
                "description": (
                    "Notable role×domain transitions. Leave empty array if none. "
                    "Include approximate year when inferable from position dates."
                ),
            },
        },
    },
}

PASS1_SYSTEM = """\
You are an expert analyst of elite career trajectories in international affairs.

Your task is PURELY DESCRIPTIVE. For the career history provided, extract what \
functional roles this person held and how they evolved. Do NOT classify or label mobility.

## Key Concept: Function = Role × Domain

A "function" is the intersection of *what role the person plays* and *what substantive domain \
they work in*:
- An academic economist and a health economist have different functions (different domain)
- A health practitioner and a health minister have different functions (different role type)
- A parliamentarian who shifts from health committees to environmental committees has \
undergone a functional transition (same role class, different substantive domain)

## Extraction Rules

1. Time-bound positions are the PRIMARY signal. Undated positions carry less weight.
2. Use `position_tags.domain` (domain array) and `position_tags.function` as strong signals.
3. Existing `functional_tags` and `career_domain`/`career_typology` are SUPPLEMENTARY only.
4. Be CONSERVATIVE about what counts as a distinct function:
   - A politician who publishes op-eds → one function (politics)
   - A diplomat who also teaches part-time → one function (diplomacy)
   - A doctor who became a minister → two functions
5. Count `raw_transition_count` strictly: the number of genuine role×domain shifts.
6. In `functional_arc`, be factual and specific. Name the actual role types and domains. \
Do NOT use mobility classification language.
"""

# ── Pass 2 Tool ───────────────────────────────────────────────────────────────

PASS2_TOOL = {
    "name": "record_comparative_classifications",
    "description": (
        "Assign mobility_type labels to all 75 HLP members based on comparative "
        "assessment of their functional profiles relative to the full corpus."
    ),
    "input_schema": {
        "type": "object",
        "required": ["classifications"],
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["person_id", "mobility_type", "confidence"],
                    "properties": {
                        "person_id": {
                            "type": "integer",
                            "description": "Must match the person_id provided in the input profile.",
                        },
                        "mobility_type": {
                            "type": "string",
                            "enum": ["monofunctional", "bifunctional",
                                     "multifunctional", "transitional"],
                        },
                        "confidence": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                        },
                        "calibration_note": {
                            "type": "string",
                            "description": (
                                "1-2 sentences explaining the label relative to the corpus. "
                                "Reference what distinguishes this person from "
                                "higher or lower mobility peers."
                            ),
                        },
                    },
                },
                "description": "Must contain exactly one entry per person_id in the input.",
            },
        },
    },
}

PASS2_SYSTEM = """\
You are an expert analyst of elite career trajectories in international affairs.

You will receive functional profiles for all 75 members of four UN Secretary-General \
High-Level Panels. Your task is to assign a COMPARATIVE mobility_type label to each person \
based on their position within this specific corpus.

## Mobility Types (relative to this corpus)

- **monofunctional**: One consistent role×domain combination throughout. \
An academic who also founded a research institute is still monofunctional — both roles \
are in the same function (academic research). Example: career academic economist.

- **bifunctional**: Two substantively distinct role×domain combinations, both with \
meaningful career engagement. Can be parallel or sequential. \
Example: journalist who became a politician; doctor who became a health administrator.

- **multifunctional**: Three or more distinct functions, OR two functions with a \
significant domain trajectory within one or both. \
Example: doctor → health minister → environmental politician.

- **transitional**: Clean before/after career switch. Phase A and Phase B are clearly \
distinct, there is little ongoing parallel engagement, and the person effectively \
"left" one function for another. Similar to bifunctional but emphasizes the \
discontinuity and directionality of the shift.

## Calibration Principles

1. **Compare relative to the corpus, not in isolation.** An academic who advises \
governments on economics is LOW mobility relative to someone who moved from medicine \
to environmental governance.
2. **Use the full vocabulary.** Expect a meaningful spread: some monofunctional, \
many bifunctional/transitional, fewer multifunctional.
3. **raw_transition_count** from pass 1 is a helpful anchor: 0 → monofunctional, \
1 → bifunctional or transitional, 2+ → multifunctional or very mobile bifunctional.
4. **Domain depth matters.** If someone's "two functions" are actually in the same \
narrow domain (e.g., two types of economic advisory roles), consider monofunctional.
5. **Be consistent.** Similar career patterns across different persons should receive \
the same label.
"""

# ── DB helpers ────────────────────────────────────────────────────────────────

def get_or_create_run(cur) -> int:
    cur.execute(
        "SELECT run_id FROM prosopography.derivative_runs WHERE run_name = %s",
        (RUN_META["run_name"],)
    )
    row = cur.fetchone()
    if row:
        print(f"  run '{RUN_META['run_name']}' already exists (run_id={row[0]})")
        return row[0]
    cur.execute("""
        INSERT INTO prosopography.derivative_runs
            (run_name, derivative_type, entity_level, model_used, prompt_version,
             narrative, replication_notes, evaluation_status, scope_json, run_timestamp)
        VALUES (%(run_name)s, %(derivative_type)s, %(entity_level)s, %(model_used)s,
                %(prompt_version)s, %(narrative)s, %(replication_notes)s,
                %(evaluation_status)s, %(scope_json)s, %(run_timestamp)s)
        RETURNING run_id
    """, {
        **RUN_META,
        "scope_json": json.dumps(RUN_META["scope_json"]),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    })
    run_id = cur.fetchone()[0]
    print(f"  created run '{RUN_META['run_name']}' run_id={run_id}")
    return run_id


def load_all_persons(conn) -> list[dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT person_id, display_name
        FROM prosopography.persons
        ORDER BY display_name
    """)
    rows = [{"person_id": r[0], "display_name": r[1]} for r in cur.fetchall()]
    cur.close()
    return rows


def get_processed_ids(conn, run_id: int, attribute_name: str) -> set[int]:
    cur = conn.cursor()
    cur.execute("""
        SELECT person_id FROM prosopography.person_attributes
        WHERE attribute_name = %s AND run_id = %s
    """, (attribute_name, run_id))
    ids = {r[0] for r in cur.fetchall()}
    cur.close()
    return ids


def fetch_person_data(conn, person_id: int) -> dict:
    cur = conn.cursor()

    cur.execute("""
        SELECT
            cp.title,
            cp.organization,
            cp.time_start,
            cp.time_finish,
            cp.approximate_period,
            cp.role_type,
            pt.domain,
            pt.function       AS tag_function,
            pt.career_phase,
            pt.geographic_scope
        FROM prosopography.career_positions cp
        LEFT JOIN prosopography.position_tags pt ON pt.position_id = cp.position_id
        WHERE cp.person_id = %s
        ORDER BY cp.time_start NULLS LAST, cp.sort_order
    """, (person_id,))
    cols = [d[0] for d in cur.description]
    positions = [dict(zip(cols, row)) for row in cur.fetchall()]

    cur.execute("""
        SELECT tags FROM prosopography.user_functional_tags
        WHERE entity_type = 'person' AND entity_id = %s
    """, (person_id,))
    ftrow = cur.fetchone()
    functional_tags = ftrow[0] if ftrow else []

    cur.execute("""
        SELECT attribute_name, attribute_value
        FROM prosopography.person_attributes
        WHERE person_id = %s
          AND attribute_name IN ('career_domain', 'career_typology', 'is_hybrid_domain')
          AND is_primary = true
    """, (person_id,))
    context_attrs = {r[0]: r[1] for r in cur.fetchall()}

    cur.close()
    return {
        "positions":       positions,
        "functional_tags": functional_tags,
        "context_attrs":   context_attrs,
    }


def load_pass1_profiles(conn, run_id: int) -> dict[int, dict]:
    """Returns {person_id: extra_data_dict} for all functional_profile rows."""
    cur = conn.cursor()
    cur.execute("""
        SELECT person_id, extra_data
        FROM prosopography.person_attributes
        WHERE attribute_name = 'functional_profile' AND run_id = %s
    """, (run_id,))
    result = {}
    for row in cur.fetchall():
        pid, extra = row
        result[pid] = extra if extra else {}
    cur.close()
    return result


# ── Prompt formatting ─────────────────────────────────────────────────────────

def format_position(pos: dict) -> str:
    parts = []
    if pos["time_start"] and pos["time_finish"]:
        parts.append(f"{pos['time_start']}–{pos['time_finish']}")
    elif pos["time_start"]:
        parts.append(f"{pos['time_start']}–?")
    elif pos["approximate_period"]:
        parts.append(f"~{pos['approximate_period']}")
    else:
        parts.append("(undated)")

    parts.append(f" {pos['title']}")
    if pos["organization"]:
        parts.append(f"@ {pos['organization']}")

    tags = []
    if pos["domain"]:
        tags.append(f"domain={pos['domain']}")
    if pos["tag_function"]:
        tags.append(f"function={pos['tag_function']}")
    if pos["role_type"]:
        tags.append(f"role_type={pos['role_type']}")
    if pos["geographic_scope"]:
        tags.append(f"geo={pos['geographic_scope']}")
    if pos["career_phase"]:
        tags.append(f"phase={pos['career_phase']}")
    if tags:
        parts.append(f"  [{', '.join(tags)}]")

    return "".join(parts)


def build_pass1_message(name: str, data: dict) -> str:
    lines = [f"## {name}\n"]

    timed = [p for p in data["positions"] if p["time_start"]]
    undated = [p for p in data["positions"] if not p["time_start"]]

    lines.append("### Career Positions (chronological)")
    for pos in timed:
        lines.append("  " + format_position(pos))
    if undated:
        lines.append("\n  **Undated / approximate:**")
        for pos in undated:
            lines.append("  " + format_position(pos))

    if data["functional_tags"]:
        lines.append(f"\n### Functional tags (supplementary): {', '.join(data['functional_tags'])}")
    if data["context_attrs"]:
        lines.append("\n### Context attributes (supplementary):")
        for k, v in data["context_attrs"].items():
            lines.append(f"  {k}: {v}")

    lines.append(
        "\n\nExtract this person's functional profile using the "
        "`record_functional_profile` tool. Be factual and descriptive only."
    )
    return "\n".join(lines)


def build_pass2_message(persons: list[dict], profiles: dict[int, dict]) -> str:
    lines = [
        "Below are functional profiles for all 75 HLP corpus members.\n"
        "Assign a comparative mobility_type label to EVERY person using "
        "`record_comparative_classifications`.\n"
        "You must return exactly 75 entries, one per person_id.\n\n"
        "---\n"
    ]

    for person in persons:
        pid = person["person_id"]
        name = person["display_name"]
        profile = profiles.get(pid, {})

        lines.append(f"**person_id={pid}  {name}**")

        funcs = profile.get("primary_functions", [])
        if funcs:
            lines.append(f"  functions: {', '.join(funcs)}")

        traj = profile.get("domain_trajectory", [])
        if traj:
            lines.append(f"  domain_arc: {' → '.join(traj)}")

        tc = profile.get("raw_transition_count")
        if tc is not None:
            lines.append(f"  raw_transitions: {tc}")

        arc = profile.get("functional_arc", "")
        if arc:
            lines.append(f"  arc: {arc}")

        transitions = profile.get("key_transitions", [])
        for t in transitions:
            yr = f" (~{t['approximate_year']})" if t.get("approximate_year") else ""
            lines.append(f"  transition: {t.get('from','?')} → {t.get('to','?')}{yr}")

        lines.append("")

    return "\n".join(lines)


# ── API calls ─────────────────────────────────────────────────────────────────

def call_pass1(client: anthropic.Anthropic, name: str, data: dict) -> dict:
    msg = build_pass1_message(name, data)
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=PASS1_SYSTEM,
        tools=[PASS1_TOOL],
        tool_choice={"type": "tool", "name": "record_functional_profile"},
        messages=[{"role": "user", "content": msg}],
    )
    return response.content[0].input


def call_pass2(client: anthropic.Anthropic, persons: list[dict],
               profiles: dict[int, dict]) -> list[dict]:
    msg = build_pass2_message(persons, profiles)
    response = client.messages.create(
        model=MODEL,
        max_tokens=16000,
        system=PASS2_SYSTEM,
        tools=[PASS2_TOOL],
        tool_choice={"type": "tool", "name": "record_comparative_classifications"},
        messages=[{"role": "user", "content": msg}],
    )
    # Find the tool use block (may not be content[0] if there's a text preamble)
    tool_block = next(
        (b for b in response.content if getattr(b, "type", None) == "tool_use"),
        None,
    )
    if tool_block is None:
        # Dump response for debugging
        print(f"  DEBUG stop_reason={response.stop_reason}")
        for i, block in enumerate(response.content):
            print(f"  DEBUG content[{i}] type={getattr(block, 'type', '?')} "
                  f"text={getattr(block, 'text', '')[:200]}")
        raise ValueError("No tool_use block found in pass 2 response")
    return tool_block.input["classifications"]


# ── Pass 1 ────────────────────────────────────────────────────────────────────

def run_pass1(conn, client: anthropic.Anthropic, run_id: int,
              persons: list[dict]) -> None:
    print("\n-- Pass 1: Individual Functional Profiling --")

    already_done = get_processed_ids(conn, run_id, "functional_profile")
    to_process = [p for p in persons if p["person_id"] not in already_done]

    print(f"  Total persons  : {len(persons)}")
    print(f"  Already done   : {len(already_done)}")
    print(f"  To process     : {len(to_process)}")

    if not to_process:
        print("  → All profiles complete. Skipping pass 1.")
        return

    errors: list[str] = []
    cur = conn.cursor()

    for i, person in enumerate(to_process, 1):
        pid = person["person_id"]
        name = person["display_name"]
        print(f"  [{i:02d}/{len(to_process):02d}] {name} ...", end=" ", flush=True)

        try:
            data = fetch_person_data(conn, pid)
            result = call_pass1(client, name, data)

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO prosopography.person_attributes
                    (person_id, run_id, attribute_name, attribute_value,
                     is_primary, extra_data)
                VALUES %s
                ON CONFLICT (person_id, run_id, attribute_name) DO NOTHING
                """,
                [(
                    pid,
                    run_id,
                    "functional_profile",
                    str(result.get("raw_transition_count", 0)),
                    True,
                    json.dumps({
                        "primary_functions":    result.get("primary_functions", []),
                        "domain_trajectory":    result.get("domain_trajectory", []),
                        "raw_transition_count": result.get("raw_transition_count", 0),
                        "functional_arc":       result.get("functional_arc", ""),
                        "key_transitions":      result.get("key_transitions", []),
                    }),
                )],
            )
            conn.commit()

            tc = result.get("raw_transition_count", 0)
            funcs = result.get("primary_functions", [])
            print(f"{tc} transitions  [{', '.join(funcs[:3])}{'…' if len(funcs) > 3 else ''}]")

            if i < len(to_process):
                time.sleep(0.5)

        except Exception as e:
            conn.rollback()
            errors.append(f"{name}: {e}")
            print(f"ERROR: {e}")

    cur.close()

    if errors:
        print(f"\n  Errors in pass 1 ({len(errors)}):")
        for err in errors:
            print(f"    {err}")


# ── Pass 2 ────────────────────────────────────────────────────────────────────

def run_pass2(conn, client: anthropic.Anthropic, run_id: int,
              persons: list[dict]) -> None:
    print("\n-- Pass 2: Comparative Classification --")

    already_done = get_processed_ids(conn, run_id, "functional_summary")
    if len(already_done) >= len(persons):
        print("  → All persons already classified. Skipping pass 2.")
        return

    if already_done:
        print(f"  Note: {len(already_done)} persons already have functional_summary — "
              f"re-running pass 2 for full corpus.")

    # Load all pass 1 profiles
    profiles = load_pass1_profiles(conn, run_id)
    missing_profiles = [p for p in persons if p["person_id"] not in profiles]
    if missing_profiles:
        print(f"  WARNING: {len(missing_profiles)} persons missing functional_profile — "
              f"pass 1 may not be complete.")
        for p in missing_profiles[:5]:
            print(f"    {p['display_name']}")

    print(f"  Sending {len(persons)} profiles to Claude for comparative classification...")

    try:
        classifications = call_pass2(client, persons, profiles)
    except Exception as e:
        print(f"  ERROR calling pass 2: {e}")
        return

    # Validate coverage
    returned_ids = {c["person_id"] for c in classifications}
    expected_ids = {p["person_id"] for p in persons}
    missing_ids = expected_ids - returned_ids
    if missing_ids:
        print(f"  WARNING: {len(missing_ids)} person_ids missing from pass 2 response.")

    # Build lookup by person_id
    class_by_id = {c["person_id"]: c for c in classifications}

    # Insert results
    cur = conn.cursor()
    rows = []
    for person in persons:
        pid = person["person_id"]
        cls = class_by_id.get(pid)
        if not cls:
            continue
        mt = cls["mobility_type"]
        rows.append((
            pid,
            run_id,
            "functional_summary",
            mt,
            MOBILITY_LABELS.get(mt, mt.capitalize()),
            cls.get("confidence"),
            True,
            json.dumps({
                "calibration_note": cls.get("calibration_note", ""),
            }),
        ))

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO prosopography.person_attributes
            (person_id, run_id, attribute_name, attribute_value,
             attribute_label, confidence, is_primary, extra_data)
        VALUES %s
        ON CONFLICT (person_id, run_id, attribute_name) DO NOTHING
        """,
        rows,
    )
    conn.commit()

    # Update n_processed
    cur.execute("""
        UPDATE prosopography.derivative_runs
        SET n_processed = (
            SELECT COUNT(*) FROM prosopography.person_attributes
            WHERE attribute_name = 'functional_summary' AND run_id = %s
        )
        WHERE run_id = %s
    """, (run_id, run_id))
    conn.commit()
    cur.close()

    print(f"  Inserted {len(rows)} functional_summary rows.")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    conn = get_connection()
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    try:
        cur = conn.cursor()
        run_id = get_or_create_run(cur)
        conn.commit()
        cur.close()

        persons = load_all_persons(conn)
        print(f"\nCorpus: {len(persons)} persons")

        run_pass1(conn, client, run_id, persons)
        run_pass2(conn, client, run_id, persons)

        # Summary
        cur = conn.cursor()
        cur.execute("""
            SELECT attribute_value, COUNT(*)
            FROM prosopography.person_attributes
            WHERE attribute_name = 'functional_summary' AND run_id = %s
            GROUP BY attribute_value
            ORDER BY COUNT(*) DESC
        """, (run_id,))
        rows = cur.fetchall()
        cur.close()

        print("\n-- Functional Mobility Distribution --")
        for mt, n in rows:
            print(f"  {mt:<20} {n}")
        print(f"  {'TOTAL':<20} {sum(r[1] for r in rows)}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

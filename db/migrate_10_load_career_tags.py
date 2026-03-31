"""
migrate_10_load_career_tags.py
--------------------------------
Loads career-position tags from:
  v3/analysis/career_tags/outputs/tagged/{Name}_tagged.json
into:
  prosopography.position_tags

Matching strategy (per position in each tagged file):
  1. Exact  — organization + title + time_start match against DB
  2. Fuzzy  — same org + time_start, title fuzzy-match (rapidfuzz >= 85)
  3. Unmatched — logged and skipped (not inserted)

Run once; re-running is safe due to UNIQUE (position_id, run_id) constraint
(will SKIP already-inserted rows via ON CONFLICT DO NOTHING).
"""

import sys
import os
import json
import csv
from pathlib import Path

import psycopg2.extras
from rapidfuzz import fuzz

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

# ── Paths ─────────────────────────────────────────────────────────────────
V3_ROOT = Path(r"C:\Users\spatt\Desktop\eliteresearchagent_v3")
TAGGED_DIR = V3_ROOT / "analysis" / "career_tags" / "outputs" / "tagged"

FUZZY_THRESHOLD = 85

# ── Derivative run metadata (best-effort for historical run) ───────────────
RUN_META = {
    "run_name":         "career_tags_v1",
    "derivative_type":  "position_tagging",
    "entity_level":     "position",
    "model_used":       "claude-sonnet-4-6",   # from v3 pipeline config
    "prompt_version":   "v1",
    "narrative": (
        "Career positions for all 75 HLP members were tagged across 8 analytical "
        "dimensions (domain, organization_type, UN_placement, geographic_scope, "
        "role_type, function, career_phase, policy_bridge) using an LLM pipeline "
        "in v3. Tags were generated from career_history.json files via the "
        "career_tags analysis service. This migration loads those tags into the "
        "prosopography DB, linking each tag row to the corresponding position_id "
        "via (organization + title + time_start) matching."
    ),
    "replication_notes": (
        "Original pipeline: eliteresearchagent_v3/analysis/career_tags/. "
        "Re-run the tagging service against career_positions in the DB to "
        "generate a new derivative_runs entry with a higher version number."
    ),
    "evaluation_status": "draft",
    "scope_json": {"hlp_panels": [1, 2, 3, 4], "n_persons": 75, "filter": "all"},
}


def normalize(s: str) -> str:
    return " ".join(s.lower().split()) if s else ""


def load_person_positions(conn) -> dict[str, list[dict]]:
    """
    Returns {person_dir_name: [{position_id, title, organization, time_start}, ...]}
    for all base career positions.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT p.person_dir_name, cp.position_id, cp.title, cp.organization, cp.time_start
        FROM prosopography.career_positions cp
        JOIN prosopography.persons p ON p.person_id = cp.person_id
        WHERE cp.data_status = 'base'
        ORDER BY p.person_dir_name, cp.sort_order
    """)
    result: dict[str, list[dict]] = {}
    for row in cur.fetchall():
        name, pid, title, org, ts = row
        result.setdefault(name, []).append({
            "position_id": pid,
            "title": title or "",
            "organization": org or "",
            "time_start": ts,
        })
    cur.close()
    return result


def match_position(
    tagged_pos: dict,
    db_positions: list[dict],
) -> tuple[int | None, str | None]:
    """
    Returns (position_id, match_method) or (None, None) if unmatched.
    """
    t_title = normalize(tagged_pos.get("title", ""))
    t_org   = normalize(tagged_pos.get("organization", ""))
    t_start = tagged_pos.get("time_start")

    # Tier 1: exact (org + title + time_start)
    for db_p in db_positions:
        if (
            normalize(db_p["title"]) == t_title
            and normalize(db_p["organization"]) == t_org
            and db_p["time_start"] == t_start
        ):
            return db_p["position_id"], "exact"

    # Tier 2: fuzzy title, same org + time_start
    candidates = [
        p for p in db_positions
        if normalize(p["organization"]) == t_org and p["time_start"] == t_start
    ]
    for db_p in candidates:
        score = fuzz.token_sort_ratio(t_title, normalize(db_p["title"]))
        if score >= FUZZY_THRESHOLD:
            return db_p["position_id"], "fuzzy"

    return None, None


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()

        # ── 1. Insert derivative_runs row ──────────────────────────────────
        # Check if run already exists
        cur.execute(
            "SELECT run_id FROM prosopography.derivative_runs WHERE run_name = %s",
            (RUN_META["run_name"],)
        )
        existing = cur.fetchone()
        if existing:
            run_id = existing[0]
            print(f"migrate_10: derivative_run '{RUN_META['run_name']}' already exists "
                  f"(run_id={run_id}). Continuing with ON CONFLICT DO NOTHING.")
        else:
            # Use the generated_at from the first tagged file as run_timestamp
            first_file = sorted(TAGGED_DIR.glob("*_tagged.json"))[0]
            with open(first_file, encoding="utf-8") as f:
                sample = json.load(f)
            run_ts = sample.get("generated_at", "2026-01-01T00:00:00+00:00")

            cur.execute("""
                INSERT INTO prosopography.derivative_runs
                    (run_name, derivative_type, entity_level, model_used, prompt_version,
                     narrative, replication_notes, evaluation_status, scope_json, run_timestamp)
                VALUES (%(run_name)s, %(derivative_type)s, %(entity_level)s, %(model_used)s,
                        %(prompt_version)s, %(narrative)s, %(replication_notes)s,
                        %(evaluation_status)s, %(scope_json)s, %(run_timestamp)s)
                RETURNING run_id
            """, {**RUN_META, "run_timestamp": run_ts, "scope_json": json.dumps(RUN_META["scope_json"])})
            run_id = cur.fetchone()[0]
            print(f"migrate_10: created derivative_run '{RUN_META['run_name']}' run_id={run_id}")

        # ── 2. Load DB positions index ─────────────────────────────────────
        person_positions = load_person_positions(conn)
        print(f"migrate_10: loaded positions for {len(person_positions)} persons from DB")

        # ── 3. Process each tagged file ────────────────────────────────────
        tagged_files = sorted(TAGGED_DIR.glob("*_tagged.json"))
        print(f"migrate_10: found {len(tagged_files)} tagged files")

        stats = {"exact": 0, "fuzzy": 0, "unmatched": 0, "inserted": 0, "skipped_conflict": 0}
        unmatched_rows: list[dict] = []

        insert_batch: list[dict] = []

        for fpath in tagged_files:
            person_dir = fpath.stem.replace("_tagged", "")  # e.g. "Abhijit_Banerjee"
            db_positions = person_positions.get(person_dir)
            if db_positions is None:
                print(f"  WARN: no DB positions for person_dir '{person_dir}' — skipping file")
                continue

            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)

            for tagged_pos in data.get("career_positions", []):
                position_id, method = match_position(tagged_pos, db_positions)
                tags = tagged_pos.get("tags", {})

                if position_id is None:
                    stats["unmatched"] += 1
                    unmatched_rows.append({
                        "person_dir": person_dir,
                        "title": tagged_pos.get("title", ""),
                        "organization": tagged_pos.get("organization", ""),
                        "time_start": tagged_pos.get("time_start"),
                    })
                    continue

                stats[method] += 1
                insert_batch.append({
                    "position_id":      position_id,
                    "run_id":           run_id,
                    "domain":           tags.get("domain"),          # list or None
                    "organization_type": tags.get("organization_type"),
                    "un_placement":     tags.get("UN_placement"),
                    "geographic_scope": tags.get("geographic_scope"),
                    "role_type":        tags.get("role_type"),
                    "function":         tags.get("function"),
                    "career_phase":     tags.get("career_phase"),
                    "policy_bridge":    tags.get("policy_bridge"),
                })

        # ── 4. Bulk insert ─────────────────────────────────────────────────
        if insert_batch:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO prosopography.position_tags
                    (position_id, run_id, domain, organization_type, un_placement,
                     geographic_scope, role_type, function, career_phase, policy_bridge)
                VALUES %s
                ON CONFLICT (position_id, run_id) DO NOTHING
                """,
                [(
                    r["position_id"], r["run_id"],
                    r["domain"],
                    r["organization_type"], r["un_placement"],
                    r["geographic_scope"], r["role_type"],
                    r["function"], r["career_phase"],
                    r["policy_bridge"],
                ) for r in insert_batch],
                template="(%s, %s, %s::text[], %s, %s, %s, %s, %s, %s, %s)",
                page_size=500,
            )
            # Count what was actually inserted vs skipped
            stats["inserted"] = cur.rowcount if cur.rowcount != -1 else len(insert_batch)

        conn.commit()

        # ── 5. Report ──────────────────────────────────────────────────────
        total = stats["exact"] + stats["fuzzy"] + stats["unmatched"]
        match_rate = (stats["exact"] + stats["fuzzy"]) / total * 100 if total else 0
        print(f"\nmigrate_10 results:")
        print(f"  Total positions in tagged files : {total}")
        print(f"  Exact matches                   : {stats['exact']}")
        print(f"  Fuzzy matches                   : {stats['fuzzy']}")
        print(f"  Unmatched (skipped)             : {stats['unmatched']}")
        print(f"  Match rate                      : {match_rate:.1f}%")
        print(f"  Rows inserted into position_tags: {stats['inserted']}")

        if unmatched_rows:
            out_csv = Path(__file__).parent / "migrate_10_unmatched.csv"
            with open(out_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["person_dir", "title", "organization", "time_start"])
                writer.writeheader()
                writer.writerows(unmatched_rows)
            print(f"  Unmatched positions written to: {out_csv}")

        # ── 6. Update n_processed ──────────────────────────────────────────
        cur = conn.cursor()
        cur.execute(
            "UPDATE prosopography.derivative_runs SET n_processed = %s WHERE run_id = %s",
            (stats["exact"] + stats["fuzzy"], run_id)
        )
        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

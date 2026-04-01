"""
resolve_parent_orgs.py
-----------------------------------
Automated Phase 1 of the parent org hierarchy normalization.

Scans all org_ontology_mappings rows where parent_org IS NOT NULL
and parent_org_id IS NULL, then attempts to resolve each unique text
value to an organizations.org_id FK.

Matching strategy (priority order):
  1. exact_name  — canonical_name ILIKE parent_org text
  2. alias       — organization_aliases.alias ILIKE parent_org text
  3. stripped    — canonical_name after removing trailing parentheticals
                   e.g. "World Bank Group (WBG)" → "World Bank Group"

Unresolved values are printed for follow-up in the UI resolution queue.

Usage:
    python resolve_parent_orgs.py [--run-id N] [--dry-run]

    --run-id   : specify which run to process (default: auto-detects most
                 recent derivative run with scope_json->>'category' = 'io_non_un')
    --dry-run  : print matches without writing to DB
"""

import sys
import os
import re
import argparse

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection


def strip_parenthetical(name: str) -> str:
    """Remove trailing parenthetical: 'World Bank Group (WBG)' → 'World Bank Group'."""
    return re.sub(r'\s*\([^)]*\)\s*$', '', name).strip()


def find_org_id(cur, text: str) -> tuple[int | None, str]:
    """
    Try to resolve a parent_org text to an org_id.
    Returns (org_id, match_method) or (None, '').
    """
    # 1. Exact canonical_name match
    cur.execute("""
        SELECT org_id FROM prosopography.organizations
        WHERE canonical_name ILIKE %(text)s
        LIMIT 1
    """, {"text": text})
    row = cur.fetchone()
    if row:
        return row[0], "exact_name"

    # 2. Alias match
    cur.execute("""
        SELECT oa.org_id FROM prosopography.organization_aliases oa
        WHERE oa.alias ILIKE %(text)s
        LIMIT 1
    """, {"text": text})
    row = cur.fetchone()
    if row:
        return row[0], "alias"

    # 3. Stripped canonical_name match (after removing parentheticals)
    stripped = strip_parenthetical(text)
    if stripped and stripped != text and len(stripped) >= 4:
        cur.execute("""
            SELECT org_id FROM prosopography.organizations
            WHERE canonical_name ILIKE %(stripped)s
            LIMIT 1
        """, {"stripped": stripped})
        row = cur.fetchone()
        if row:
            return row[0], "stripped"

    return None, ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve parent_org text → parent_org_id FK in org_ontology_mappings"
    )
    parser.add_argument(
        "--run-id", type=int, default=None,
        help="Run ID to process (default: auto-detect most recent io_non_un run)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report results without writing to DB",
    )
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # ── Resolve run_id ────────────────────────────────────────────────────────
    run_id = args.run_id
    if run_id is None:
        cur.execute("""
            SELECT run_id, run_name FROM prosopography.derivative_runs
            WHERE scope_json->>'category' = 'io_non_un'
            ORDER BY run_id DESC LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            print("ERROR: No io_non_un derivative run found. Specify --run-id manually.")
            cur.close()
            conn.close()
            sys.exit(1)
        run_id, run_name = row
        print(f"Auto-detected run_id={run_id} ({run_name})")

    # ── Get all unresolved parent_org values ──────────────────────────────────
    cur.execute("""
        SELECT DISTINCT parent_org, COUNT(*) AS n
        FROM prosopography.org_ontology_mappings
        WHERE run_id = %(run_id)s
          AND parent_org IS NOT NULL
          AND parent_org_id IS NULL
        GROUP BY parent_org
        ORDER BY parent_org
    """, {"run_id": run_id})
    rows = cur.fetchall()

    if not rows:
        print("Nothing to resolve — all parent_org values already have parent_org_id set.")
        cur.close()
        conn.close()
        return

    print(f"\nFound {len(rows)} unique unresolved parent_org value(s):\n")

    resolved_count = 0
    unresolved_report: list[tuple[str, int]] = []

    for (text, mapping_count) in rows:
        org_id, method = find_org_id(cur, text)

        if org_id:
            cur.execute(
                "SELECT canonical_name FROM prosopography.organizations WHERE org_id = %s",
                (org_id,),
            )
            canonical = cur.fetchone()[0]
            print(
                f"  [RESOLVED]   '{text}'"
                f" -> org_id={org_id} '{canonical}'"
                f" (method: {method}, {mapping_count} mapping(s))"
            )
            if not args.dry_run:
                cur.execute("""
                    UPDATE prosopography.org_ontology_mappings
                    SET parent_org_id = %(org_id)s, updated_at = now()
                    WHERE run_id = %(run_id)s
                      AND parent_org = %(text)s
                      AND parent_org_id IS NULL
                """, {"org_id": org_id, "run_id": run_id, "text": text})
            resolved_count += 1
        else:
            print(f"  [UNRESOLVED] '{text}' ({mapping_count} mapping(s)) — needs manual review in UI")
            unresolved_report.append((text, mapping_count))

    # ── Commit or report ──────────────────────────────────────────────────────
    if not args.dry_run and resolved_count > 0:
        conn.commit()
        print(f"\nCommitted {resolved_count} resolution(s) to DB.")
    elif args.dry_run:
        print(f"\n[DRY RUN] Would resolve {resolved_count} value(s). No changes written.")
    else:
        print(f"\nNo new resolutions found.")

    if unresolved_report:
        total_unresolved_mappings = sum(n for _, n in unresolved_report)
        print(
            f"\n{len(unresolved_report)} text value(s) unresolved"
            f" ({total_unresolved_mappings} mapping(s))."
            f" Use the 'Resolve Parents' tab in the Ontology Editor to complete these manually."
        )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

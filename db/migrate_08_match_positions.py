"""
migrate_08_match_positions.py
------------------------------
Matches the raw organization TEXT in career_positions to canonical org entries,
populating career_positions.org_id and career_positions.org_match_method.

Matching tiers (in order):
  1. exact   — case-insensitive match against canonical_name
  2. alias   — case-insensitive match against organization_aliases.alias
  3. fuzzy   — rapidfuzz token_sort_ratio >= 90 against canonical names + aliases

Unmatched positions are left with org_id = NULL.
Existing non-NULL org_id values are NOT overwritten unless --force is passed.
"""

import sys
import os
import argparse

from rapidfuzz import fuzz, process as rfprocess

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

FUZZY_THRESHOLD = 90  # minimum score (0-100) to auto-accept a fuzzy match


def normalize(text: str) -> str:
    """Lowercase, strip, collapse internal whitespace."""
    return " ".join(text.lower().split())


def build_lookups(cur) -> tuple[dict, dict, list]:
    """
    Returns:
      canonical_lookup: {normalized_canonical_name: org_id}
      alias_lookup:     {normalized_alias: org_id}
      fuzzy_candidates: [(display_name, org_id), ...] for rapidfuzz
    """
    cur.execute("SELECT org_id, canonical_name FROM prosopography.organizations")
    rows = cur.fetchall()
    canonical_lookup = {normalize(name): org_id for org_id, name in rows}
    fuzzy_candidates = [(name, org_id) for org_id, name in rows]

    cur.execute("SELECT org_id, alias FROM prosopography.organization_aliases")
    alias_rows = cur.fetchall()
    alias_lookup = {normalize(alias): org_id for org_id, alias in alias_rows}

    # Add aliases to fuzzy candidates as well
    for org_id, alias in alias_rows:
        fuzzy_candidates.append((alias, org_id))

    return canonical_lookup, alias_lookup, fuzzy_candidates


def fuzzy_match(
    org_string: str,
    fuzzy_candidates: list[tuple[str, int]],
) -> tuple[int | None, int]:
    """
    Return (org_id, score) for the best fuzzy match, or (None, 0) if below threshold.
    Uses token_sort_ratio which handles word-order differences.
    """
    names = [c[0] for c in fuzzy_candidates]
    result = rfprocess.extractOne(
        org_string,
        names,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=FUZZY_THRESHOLD,
    )
    if result is None:
        return None, 0
    best_name, score, idx = result
    return fuzzy_candidates[idx][1], score


def main(force: bool = False) -> None:
    conn = get_connection()
    cur = conn.cursor()

    print("Building org lookup indexes...")
    canonical_lookup, alias_lookup, fuzzy_candidates = build_lookups(cur)
    print(f"  {len(canonical_lookup)} canonical names, {len(alias_lookup)} aliases, "
          f"{len(fuzzy_candidates)} fuzzy candidates total.")

    # Fetch all distinct org strings that need matching
    if force:
        cur.execute("""
            SELECT DISTINCT organization
            FROM prosopography.career_positions
            WHERE organization IS NOT NULL AND organization <> ''
        """)
    else:
        cur.execute("""
            SELECT DISTINCT organization
            FROM prosopography.career_positions
            WHERE organization IS NOT NULL AND organization <> ''
              AND org_id IS NULL
        """)
    org_strings = [r[0] for r in cur.fetchall()]
    print(f"Found {len(org_strings)} distinct org strings to match.")

    # Build per-string resolution map: {org_string: (org_id, method)}
    resolution: dict[str, tuple[int | None, str | None]] = {}

    exact_count = alias_count = fuzzy_count = unmatched_count = 0

    for org_str in org_strings:
        norm = normalize(org_str)

        # Tier 1: exact canonical match
        if norm in canonical_lookup:
            resolution[org_str] = (canonical_lookup[norm], "exact")
            exact_count += 1
            continue

        # Tier 2: alias match
        if norm in alias_lookup:
            resolution[org_str] = (alias_lookup[norm], "alias")
            alias_count += 1
            continue

        # Tier 3: fuzzy match
        org_id, score = fuzzy_match(org_str, fuzzy_candidates)
        if org_id is not None:
            resolution[org_str] = (org_id, "fuzzy")
            fuzzy_count += 1
            continue

        # Unmatched
        resolution[org_str] = (None, None)
        unmatched_count += 1

    print(f"\nResolution summary:")
    print(f"  exact:     {exact_count}")
    print(f"  alias:     {alias_count}")
    print(f"  fuzzy:     {fuzzy_count}")
    print(f"  unmatched: {unmatched_count}")
    match_rate = (exact_count + alias_count + fuzzy_count) / max(len(org_strings), 1) * 100
    print(f"  match rate: {match_rate:.1f}%")

    # Apply updates to career_positions
    print("\nUpdating career_positions.org_id...")
    updated = 0
    for org_str, (org_id, method) in resolution.items():
        if org_id is None:
            continue
        cur.execute(
            """
            UPDATE prosopography.career_positions
            SET org_id = %s, org_match_method = %s
            WHERE organization = %s AND (org_id IS NULL OR %s)
            """,
            (org_id, method, org_str, force),
        )
        updated += cur.rowcount

    conn.commit()

    # Summary query
    cur.execute("""
        SELECT
            org_match_method,
            COUNT(*) as n,
            COUNT(DISTINCT organization) as distinct_orgs
        FROM prosopography.career_positions
        GROUP BY org_match_method
        ORDER BY org_match_method NULLS LAST
    """)
    print("\ncareer_positions match breakdown:")
    print(f"  {'method':<12} {'rows':>6}  {'distinct orgs':>14}")
    print("  " + "-" * 36)
    for method, n, d_orgs in cur.fetchall():
        print(f"  {method or 'unmatched':<12} {n:>6}  {d_orgs:>14}")

    cur.close()
    conn.close()
    print(f"\nDone. {updated} career_position rows updated with org_id.")


def create_stubs(conn) -> int:
    """
    Create stub organization entries for all career_positions rows that have
    a non-null organization string but no org_id.  Stubs get:
      meta_type = 'other', review_status = 'pending_review', source = 'auto_stub'

    Returns the number of stubs created.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT organization
        FROM prosopography.career_positions
        WHERE organization IS NOT NULL AND organization <> ''
          AND org_id IS NULL
        ORDER BY organization
    """)
    unmatched = [r[0] for r in cur.fetchall()]
    print(f"Creating stubs for {len(unmatched)} unmatched org strings...")

    created = 0
    for org_str in unmatched:
        # Insert stub — skip if canonical_name already exists (idempotent)
        cur.execute(
            """
            INSERT INTO prosopography.organizations
                (canonical_name, meta_type, review_status, source)
            VALUES (%s, 'other', 'pending_review', 'auto_stub')
            ON CONFLICT (canonical_name) DO NOTHING
            RETURNING org_id
            """,
            (org_str,),
        )
        row = cur.fetchone()
        if row:
            created += 1

    # Now link stubs to career_positions via exact match
    cur.execute("""
        UPDATE prosopography.career_positions cp
        SET org_id = o.org_id,
            org_match_method = 'exact'
        FROM prosopography.organizations o
        WHERE cp.organization = o.canonical_name
          AND cp.org_id IS NULL
    """)
    linked = cur.rowcount

    conn.commit()
    cur.close()
    print(f"  Stubs created: {created}, career_positions linked: {linked}")
    return created


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-match all positions, overwriting existing org_id values.",
    )
    parser.add_argument(
        "--create-stubs",
        action="store_true",
        help="Create stub org entries for all unmatched positions and link them.",
    )
    args = parser.parse_args()
    conn = get_connection() if args.create_stubs else None
    main(force=args.force)
    if args.create_stubs:
        conn = get_connection()
        create_stubs(conn)
        conn.close()

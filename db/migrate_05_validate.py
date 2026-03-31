"""
migrate_05_validate.py
-----------------------
Read-only integrity checks after migration. Prints a summary table and
reports any anomalies.
"""

import sys
import os

# Ensure UTF-8 output on Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

EXPECTED = {
    "hlp_panels":              4,
    "persons":                 75,
    "career_positions":        2000,  # minimum threshold (~2,183 expected)
    "education":               200,   # minimum threshold (~219 expected)
    "awards":                  600,   # minimum threshold (~715 expected)
    "pipeline_runs":           75,
    "biographical_provenance": 280,   # minimum threshold (~301 expected)
}


def check(cur, label: str, query: str, params=None) -> int:
    cur.execute(query, params or ())
    result = cur.fetchone()
    val = result[0] if result else 0
    return int(val)


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 60)
    print("MIGRATION VALIDATION REPORT")
    print("=" * 60)

    # ── Row counts ──────────────────────────────────────────────
    print("\n── Row Counts ──")
    counts = {}
    for table in ["hlp_panels", "persons", "career_positions", "education",
                  "awards", "pipeline_runs", "biographical_provenance"]:
        n = check(cur, table, f"SELECT COUNT(*) FROM prosopography.{table}")
        counts[table] = n
        min_expected = EXPECTED[table]
        status = "OK" if n >= min_expected else "WARN"
        print(f"  [{status}] {table}: {n} rows (expected >= {min_expected})")

    # ── Person nationalities ─────────────────────────────────────
    n_nat = check(cur, "person_nationalities",
                  "SELECT COUNT(*) FROM prosopography.person_nationalities")
    print(f"  [INFO] person_nationalities: {n_nat} rows")

    # ── All persons have at least 1 career_position ──────────────
    print("\n── Career Event Coverage ──")
    cur.execute("""
        SELECT p.display_name
        FROM prosopography.persons p
        LEFT JOIN prosopography.career_positions cp ON p.person_id = cp.person_id
        WHERE cp.position_id IS NULL
        ORDER BY p.display_name
    """)
    persons_no_positions = [r[0] for r in cur.fetchall()]
    if persons_no_positions:
        print(f"  [WARN] {len(persons_no_positions)} persons with NO career_positions:")
        for name in persons_no_positions:
            print(f"    - {name}")
    else:
        print("  [OK] All 75 persons have at least 1 career_position.")

    # ── sources_person_name matches sources.persons_searched ─────
    print("\n── Sources Name Coverage ──")
    cur.execute("""
        SELECT p.display_name, p.sources_person_name
        FROM prosopography.persons p
        WHERE p.sources_person_name NOT IN (
            SELECT DISTINCT person_name FROM sources.persons_searched
        )
        ORDER BY p.display_name
    """)
    unmatched = cur.fetchall()
    if unmatched:
        print(f"  [WARN] {len(unmatched)} persons NOT in sources.persons_searched:")
        for display, spn in unmatched:
            print(f"    - {display!r} (stored as: {spn!r})")
    else:
        print("  [OK] All sources_person_name values exist in sources.persons_searched.")

    # ── referenced_chunk_ids exist in sources.chunks ─────────────
    print("\n── Chunk ID Integrity ──")
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT unnest(referenced_chunk_ids) AS chunk_id
            FROM prosopography.biographical_provenance
            WHERE referenced_chunk_ids IS NOT NULL
        ) sub
        WHERE chunk_id NOT IN (SELECT id FROM sources.chunks)
    """)
    bad_chunk_ids = cur.fetchone()[0]
    if bad_chunk_ids:
        print(f"  [WARN] {bad_chunk_ids} referenced_chunk_ids not found in sources.chunks.")
    else:
        print("  [OK] All referenced_chunk_ids exist in sources.chunks.")

    # ── Deceased with no death_year ───────────────────────────────
    print("\n── Deceased Persons ──")
    cur.execute("""
        SELECT display_name FROM prosopography.persons
        WHERE death_status = 'deceased' AND death_year IS NULL
        ORDER BY display_name
    """)
    deceased_no_year = [r[0] for r in cur.fetchall()]
    if deceased_no_year:
        print(f"  [WARN] {len(deceased_no_year)} deceased persons with null death_year:")
        for name in deceased_no_year:
            print(f"    - {name}")
    else:
        print("  [OK] No deceased persons with null death_year.")

    # ── Biographical provenance coverage ─────────────────────────
    print("\n── Biographical Provenance Coverage ──")
    cur.execute("""
        SELECT p.display_name
        FROM prosopography.persons p
        LEFT JOIN prosopography.biographical_provenance bp ON p.person_id = bp.person_id
        WHERE bp.prov_id IS NULL
        ORDER BY p.display_name
    """)
    no_prov = [r[0] for r in cur.fetchall()]
    if no_prov:
        print(f"  [WARN] {len(no_prov)} persons with NO biographical provenance:")
        for name in no_prov:
            print(f"    - {name}")
    else:
        print("  [OK] All persons have at least 1 biographical provenance record.")

    # ── HLP distribution ─────────────────────────────────────────
    print("\n── HLP Distribution ──")
    cur.execute("""
        SELECT h.hlp_name, h.hlp_year, COUNT(p.person_id) AS n
        FROM prosopography.hlp_panels h
        LEFT JOIN prosopography.persons p ON h.hlp_id = p.hlp_id
        GROUP BY h.hlp_name, h.hlp_year
        ORDER BY h.hlp_year
    """)
    for hlp_name, hlp_year, n in cur.fetchall():
        print(f"  {hlp_year} {hlp_name}: {n} persons")

    # ── Orphan check ─────────────────────────────────────────────
    print("\n── Orphan Check ──")
    for table, fk_col in [
        ("career_positions", "person_id"),
        ("education", "person_id"),
        ("awards", "person_id"),
        ("pipeline_runs", "person_id"),
        ("biographical_provenance", "person_id"),
    ]:
        cur.execute(
            f"""
            SELECT COUNT(*) FROM prosopography.{table} t
            WHERE t.{fk_col} NOT IN (SELECT person_id FROM prosopography.persons)
            """
        )
        n_orphan = cur.fetchone()[0]
        status = "OK" if n_orphan == 0 else "WARN"
        print(f"  [{status}] {table}: {n_orphan} orphaned rows")

    # ── Confidence distribution (career_positions) ───────────────
    print("\n── Confidence Distribution (career_positions) ──")
    cur.execute("""
        SELECT confidence, COUNT(*) FROM prosopography.career_positions
        GROUP BY confidence ORDER BY confidence
    """)
    for conf, n in cur.fetchall():
        print(f"  {conf or 'NULL'}: {n}")

    # ── Organizations (Part 2) ────────────────────────────────────
    cur.execute("SELECT to_regclass('prosopography.organizations')")
    if cur.fetchone()[0] is not None:
        print("\n── Organizations ──")
        cur.execute("SELECT COUNT(*) FROM prosopography.organizations")
        n_orgs = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM prosopography.organization_aliases")
        n_aliases = cur.fetchone()[0]
        print(f"  organizations: {n_orgs}, aliases: {n_aliases}")

        cur.execute("""
            SELECT meta_type, COUNT(*) FROM prosopography.organizations
            GROUP BY meta_type ORDER BY meta_type
        """)
        for mt, n in cur.fetchall():
            print(f"    {mt or 'NULL'}: {n}")

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE org_id IS NOT NULL) AS linked,
                COUNT(*) FILTER (WHERE org_id IS NULL AND organization IS NOT NULL AND organization <> '') AS unlinked
            FROM prosopography.career_positions
        """)
        linked, unlinked = cur.fetchone()
        status = "OK" if unlinked == 0 else "WARN"
        print(f"  [{status}] career_positions with org_id: {linked}, without: {unlinked}")

        cur.execute("""
            SELECT org_match_method, COUNT(*) FROM prosopography.career_positions
            GROUP BY org_match_method ORDER BY org_match_method NULLS LAST
        """)
        for method, n in cur.fetchall():
            print(f"    {method or 'unmatched'}: {n}")

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Validation complete.")


if __name__ == "__main__":
    main()

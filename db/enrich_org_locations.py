"""
enrich_org_locations.py
------------------------
Enriches prosopography.organizations with city / country / region data using:
  1. Serper web search (two queries per org, deduplicated by URL)
  2. Cohere rerank (rerank-v4.0-pro) to surface location-relevant snippets
  3. Cohere LLM (command-a-03-2025, JSON mode) to extract structured location
  4. Multi-source validation (count of top-5 snippets that agree on city/country)

Results are written to prosopography.org_location_searches for full provenance.
Organizations table is updated when confidence >= 0.7 (unless --overwrite skipped).

Resume support: on restart, the most recent draft run is reused automatically and
orgs already present in org_location_searches for that run are skipped.

Usage:
  python db/enrich_org_locations.py [options]

Options:
  --run-id N    Resume a specific run by ID (default: auto-detect latest draft)
  --new-run     Force a fresh run even if a draft run exists
  --workers N   Parallel worker threads (default: 5)
  --all         Include auto-stub orgs (data_status='derivative'); default: base only
  --overwrite   Overwrite existing location values on organizations
  --dry-run     Print results without writing to DB
  --limit N     Process at most N orgs (useful for testing)
"""

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
import cohere
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SERPER_API_KEY = os.environ["SERPER_API_KEY"]
COHERE_API_KEY = os.environ["COHERE_API_KEY"]
SERPER_HEADERS = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

# Cohere client is thread-safe (stateless HTTP calls)
co = cohere.ClientV2(api_key=COHERE_API_KEY)

CONFIDENCE_THRESHOLD = 0.7
RERANK_TOP_N = 5
RUN_NAME = "org_location_enrichment_v1"


# ── Serper search ──────────────────────────────────────────────────────────────

def _serper_search(query: str, num: int = 10) -> list[dict]:
    payload = {"q": query, "num": num}
    resp = requests.post(
        "https://google.serper.dev/search",
        json=payload,
        headers=SERPER_HEADERS,
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json().get("organic", [])


def collect_snippets(org_name: str) -> tuple[list[dict], str]:
    q1 = f'"{org_name}" headquarters location city country'
    q2 = f'"{org_name}" based in where'
    seen_urls: set[str] = set()
    results: list[dict] = []
    for query in [q1, q2]:
        for item in _serper_search(query, num=10):
            url = item.get("link", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            results.append({
                "url": url,
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
            })
    return results, f"{q1} | {q2}"


# ── Cohere rerank ──────────────────────────────────────────────────────────────

def rerank_snippets(org_name: str, snippets: list[dict]) -> list[dict]:
    if not snippets:
        return []
    docs = [f"{s['title']} — {s['snippet']}" for s in snippets]
    rerank_query = f"What city and country is {org_name} headquartered in?"
    response = co.rerank(
        model="rerank-v4.0-pro",
        query=rerank_query,
        documents=docs,
        top_n=min(RERANK_TOP_N, len(docs)),
    )
    ranked = []
    for r in response.results:
        entry = dict(snippets[r.index])
        entry["rerank_score"] = r.relevance_score
        ranked.append(entry)
    return ranked


# ── Cohere LLM extraction ──────────────────────────────────────────────────────

EXTRACTION_SYSTEM = (
    "You are a data extraction assistant. Given web snippets about an organization, "
    "extract its headquarters location. Return ONLY a JSON object with keys: "
    "city (string or null), country (ISO alpha-3 string or null), "
    "region (macro-region string or null, e.g. 'Western Europe', 'East Asia', "
    "'Sub-Saharan Africa', 'North America', 'Latin America', 'Middle East', "
    "'South Asia', 'Southeast Asia', 'Central Asia', 'Oceania', 'Eastern Europe', "
    "'North Africa'), confidence (float 0.0-1.0). "
    "If information is ambiguous or absent, set confidence below 0.5."
)


def extract_location(org_name: str, top_snippets: list[dict]) -> dict:
    context = "\n\n".join(
        f"[{i+1}] {s['title']}\n{s['snippet']}" for i, s in enumerate(top_snippets)
    )
    user_msg = (
        f"Organization: {org_name}\n\n"
        f"Web snippets:\n{context}\n\n"
        "Extract the headquarters location as JSON."
    )
    response = co.chat(
        model="command-a-03-2025",
        messages=[
            {"role": "system", "content": EXTRACTION_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    raw = response.message.content[0].text
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"city": None, "country": None, "region": None, "confidence": 0.0}


# ── Multi-source validation ────────────────────────────────────────────────────

def count_source_agreement(top_snippets: list[dict], city: Optional[str], country: Optional[str]) -> int:
    if not city and not country:
        return 0
    count = 0
    for s in top_snippets:
        text = (s.get("title", "") + " " + s.get("snippet", "")).lower()
        city_match = city and city.lower() in text
        country_match = country and country.lower() in text
        if city_match or country_match:
            count += 1
    return count


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_or_create_run(conn, force_new: bool, explicit_run_id: Optional[int]) -> tuple[int, bool]:
    """Return (run_id, is_resumed). Reuses latest draft run unless force_new or explicit_run_id given."""
    with conn.cursor() as cur:
        if explicit_run_id is not None:
            cur.execute(
                "SELECT run_id FROM prosopography.derivative_runs WHERE run_id = %s",
                (explicit_run_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"run_id {explicit_run_id} not found in derivative_runs")
            return explicit_run_id, True

        if not force_new:
            cur.execute(
                """
                SELECT run_id FROM prosopography.derivative_runs
                WHERE run_name = %s AND evaluation_status = 'draft'
                ORDER BY run_timestamp DESC LIMIT 1
                """,
                (RUN_NAME,),
            )
            row = cur.fetchone()
            if row:
                return row[0], True

        cur.execute(
            """
            INSERT INTO prosopography.derivative_runs
                (run_name, derivative_type, entity_level, model_used,
                 prompt_version, evaluation_status, run_timestamp)
            VALUES (%s, %s, %s, %s, %s, 'draft', %s)
            RETURNING run_id
            """,
            (
                RUN_NAME,
                "location_enrichment",
                "organization",
                "command-a-03-2025 + rerank-v4.0-pro",
                "v1",
                datetime.now(timezone.utc),
            ),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id, False


def fetch_target_orgs(conn, run_id: int, include_all: bool, limit: Optional[int]) -> list[dict]:
    """Return orgs not yet processed in this run."""
    base_filter = "" if include_all else "AND o.data_status = 'base'"
    query = f"""
        SELECT DISTINCT o.org_id, o.canonical_name,
               o.location_city, o.location_country, o.location_region
        FROM prosopography.organizations o
        JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
        WHERE NOT EXISTS (
            SELECT 1 FROM prosopography.org_location_searches s
            WHERE s.org_id = o.org_id AND s.run_id = %s
        )
        {base_filter}
        ORDER BY o.org_id
    """
    if limit:
        query += f" LIMIT {limit}"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (run_id,))
        return [dict(r) for r in cur.fetchall()]


def write_search_row(conn, run_id: int, org_id: int, search_query: str,
                     serper_results: list[dict], top_snippets: list[dict],
                     extracted: dict, sources_validated: int) -> None:
    confidence = float(extracted.get("confidence", 0.0))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO prosopography.org_location_searches
                (run_id, org_id, search_query, serper_results, rerank_scores,
                 extracted_city, extracted_country, extracted_region,
                 confidence, sources_used, sources_validated, applied)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (org_id, run_id) DO UPDATE SET
                search_query      = EXCLUDED.search_query,
                serper_results    = EXCLUDED.serper_results,
                rerank_scores     = EXCLUDED.rerank_scores,
                extracted_city    = EXCLUDED.extracted_city,
                extracted_country = EXCLUDED.extracted_country,
                extracted_region  = EXCLUDED.extracted_region,
                confidence        = EXCLUDED.confidence,
                sources_used      = EXCLUDED.sources_used,
                sources_validated = EXCLUDED.sources_validated,
                applied           = EXCLUDED.applied
            """,
            (
                run_id,
                org_id,
                search_query,
                json.dumps(serper_results),
                json.dumps(top_snippets),
                extracted.get("city"),
                extracted.get("country"),
                extracted.get("region"),
                confidence,
                json.dumps(top_snippets),
                sources_validated,
                confidence >= CONFIDENCE_THRESHOLD,
            ),
        )
    conn.commit()


def apply_to_org(conn, org_id: int, extracted: dict, overwrite: bool, existing: dict) -> bool:
    updates: dict[str, str] = {}
    for db_col, key in [
        ("location_city", "city"),
        ("location_country", "country"),
        ("location_region", "region"),
    ]:
        val = extracted.get(key)
        if val and (overwrite or not existing.get(db_col)):
            updates[db_col] = val

    if not updates:
        return False

    set_clause = ", ".join(f"{col} = %s" for col in updates)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE prosopography.organizations SET {set_clause} WHERE org_id = %s",
            list(updates.values()) + [org_id],
        )
    conn.commit()
    return True


# ── Per-org worker ─────────────────────────────────────────────────────────────

def process_org(
    org: dict,
    run_id: int,
    overwrite: bool,
    dry_run: bool,
    print_lock: threading.Lock,
    counters: dict,
    index: int,
    total: int,
) -> None:
    org_id = org["org_id"]
    name = org["canonical_name"]

    def log(msg: str) -> None:
        with print_lock:
            print(f"[{index}/{total}] {name} ... {msg}", flush=True)

    try:
        snippets, combined_query = collect_snippets(name)
        if not snippets:
            log("no snippets found, skipping")
            with print_lock:
                counters["skipped"] += 1
            return

        top_snippets = rerank_snippets(name, snippets)
        if not top_snippets:
            log("rerank returned nothing, skipping")
            with print_lock:
                counters["skipped"] += 1
            return

        extracted = extract_location(name, top_snippets)
        confidence = float(extracted.get("confidence", 0.0))
        sources_validated = count_source_agreement(
            top_snippets, extracted.get("city"), extracted.get("country")
        )

        city = extracted.get("city") or "—"
        country = extracted.get("country") or "—"
        region = extracted.get("region") or "—"
        log(f"{city}, {country} ({region}) conf={confidence:.2f} srcs={sources_validated}")

        if dry_run:
            return

        # Each worker uses its own connection
        conn = get_connection()
        try:
            write_search_row(conn, run_id, org_id, combined_query, snippets, top_snippets, extracted, sources_validated)
            if confidence >= CONFIDENCE_THRESHOLD:
                was_applied = apply_to_org(conn, org_id, extracted, overwrite, org)
                if was_applied:
                    with print_lock:
                        counters["applied"] += 1
        finally:
            conn.close()

    except Exception as exc:
        log(f"ERROR: {exc}")
        with print_lock:
            counters["skipped"] += 1


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich org locations via Serper + Cohere")
    parser.add_argument("--run-id", type=int, default=None,
                        help="Resume a specific run by derivative_runs.run_id")
    parser.add_argument("--new-run", action="store_true",
                        help="Force a fresh run even if a draft run exists")
    parser.add_argument("--workers", type=int, default=5,
                        help="Number of parallel worker threads (default: 5)")
    parser.add_argument("--all", action="store_true", dest="include_all",
                        help="Include auto-stub orgs (data_status='derivative')")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing location values on organizations")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without writing to DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of remaining orgs to process")
    args = parser.parse_args()

    conn = get_connection()

    if args.dry_run:
        run_id = args.run_id or -1
        is_resumed = False
        print("[DRY RUN] No DB writes will occur.")
    else:
        run_id, is_resumed = get_or_create_run(conn, args.new_run, args.run_id)
        status = f"Resuming run_id={run_id}" if is_resumed else f"Created new run_id={run_id}"
        print(status)

    orgs = fetch_target_orgs(conn, run_id, args.include_all, args.limit)
    conn.close()

    if not orgs:
        print("No remaining orgs to process.")
        return

    print(f"Processing {len(orgs)} organizations with {args.workers} workers...")

    print_lock = threading.Lock()
    counters = {"applied": 0, "skipped": 0}
    total = len(orgs)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_org,
                org, run_id, args.overwrite, args.dry_run,
                print_lock, counters, i, total,
            ): org
            for i, org in enumerate(orgs, 1)
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                org = futures[future]
                with print_lock:
                    print(f"Unhandled error for {org['canonical_name']}: {exc}")

    print(f"\nDone. Applied: {counters['applied']}, Skipped/Error: {counters['skipped']}")

    if not args.dry_run:
        conn2 = get_connection()
        try:
            processed = total - counters["skipped"]
            with conn2.cursor() as cur:
                cur.execute(
                    """
                    UPDATE prosopography.derivative_runs
                    SET n_processed = COALESCE(n_processed, 0) + %s
                    WHERE run_id = %s
                    """,
                    (processed, run_id),
                )
            conn2.commit()
        finally:
            conn2.close()


if __name__ == "__main__":
    main()

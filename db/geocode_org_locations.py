"""
geocode_org_locations.py
------------------------
Geocodes unique (city, country) pairs from prosopography.organizations
using the Nominatim API, then back-fills lat/lng for all matching orgs.

Usage:
    python db/geocode_org_locations.py
    python db/geocode_org_locations.py --dry-run
    python db/geocode_org_locations.py --limit 50
    python db/geocode_org_locations.py --overwrite   # re-geocode already-filled orgs

Rate limit: 1 req/sec (Nominatim ToS). ~400 unique pairs ≈ 7 minutes.
"""

import argparse
import sys
import os
import time

import requests

sys.path.insert(0, os.path.dirname(__file__))
from db_utils import get_connection

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "EliteResearchAgent/1.0 (prosopography research tool)"
SLEEP_SEC = 1.1
VALID_TYPES = {"city", "town", "village", "administrative", "municipality", "suburb", "country"}

# ISO 3166-1 alpha-3 ->full English country name (Nominatim-friendly)
ALPHA3_TO_NAME: dict[str, str] = {
    "AFG": "Afghanistan", "ARE": "United Arab Emirates", "ARG": "Argentina",
    "AUS": "Australia", "AUT": "Austria", "AZE": "Azerbaijan",
    "BEL": "Belgium", "BEN": "Benin", "BFA": "Burkina Faso",
    "BGD": "Bangladesh", "BHR": "Bahrain", "BIH": "Bosnia and Herzegovina",
    "BRA": "Brazil", "BWA": "Botswana", "CAN": "Canada",
    "CHE": "Switzerland", "CHL": "Chile", "CHN": "China",
    "CIV": "Ivory Coast", "COD": "Democratic Republic of the Congo",
    "COL": "Colombia", "CUB": "Cuba", "CZE": "Czech Republic",
    "DEU": "Germany", "DNK": "Denmark", "EGY": "Egypt",
    "ESP": "Spain", "EST": "Estonia", "ETH": "Ethiopia",
    "FIN": "Finland", "FRA": "France", "GBR": "United Kingdom",
    "GHA": "Ghana", "GNB": "Guinea-Bissau", "GUY": "Guyana",
    "HKG": "Hong Kong", "IDN": "Indonesia", "IMN": "Isle of Man",
    "IND": "India", "IRL": "Ireland", "IRN": "Iran",
    "ISR": "Israel", "ITA": "Italy", "JOR": "Jordan",
    "JPN": "Japan", "KEN": "Kenya", "KHM": "Cambodia",
    "KOR": "South Korea", "KWT": "Kuwait", "LAT": "Latvia",
    "LBN": "Lebanon", "LBR": "Liberia", "LKA": "Sri Lanka",
    "LUX": "Luxembourg", "LVA": "Latvia", "MAC": "Macau",
    "MEX": "Mexico", "MLT": "Malta", "MOZ": "Mozambique",
    "MUS": "Mauritius", "MYS": "Malaysia", "NGA": "Nigeria",
    "NLD": "Netherlands", "NOR": "Norway", "PAK": "Pakistan",
    "PER": "Peru", "PHL": "Philippines", "POL": "Poland",
    "PRT": "Portugal", "QAT": "Qatar", "ROU": "Romania",
    "RUS": "Russia", "RWA": "Rwanda", "SAU": "Saudi Arabia",
    "SDN": "Sudan", "SEN": "Senegal", "SGP": "Singapore",
    "SLE": "Sierra Leone", "SRB": "Serbia", "SVK": "Slovakia",
    "SVN": "Slovenia", "SWE": "Sweden", "SYR": "Syria",
    "THA": "Thailand", "THAI": "Thailand", "TLS": "Timor-Leste",
    "TUR": "Turkey", "TZA": "Tanzania", "UA": "Ukraine",
    "UGA": "Uganda", "URY": "Uruguay", "USA": "United States",
    "VEN": "Venezuela", "XKX": "Kosovo", "YEM": "Yemen",
    "ZAF": "South Africa", "ZMB": "Zambia", "ZWE": "Zimbabwe",
    # Historical / special
    "ROC": "Taiwan", "YUG": "Serbia", "USSR": "Russia",
}

# Values to skip outright (ambiguous, missing, or multi-country)
SKIP_CODES = {"N/A", "unknown", "unknown ", "UNKNOWN", "IRL, GBR"}


def resolve_country_name(code: str) -> str | None:
    """Return a full country name for a raw country code, or None to skip."""
    if not code or code.strip() in SKIP_CODES:
        return None
    code = code.strip()
    if code in ALPHA3_TO_NAME:
        return ALPHA3_TO_NAME[code]
    # Fall back to using the raw string (might work for some edge cases)
    return code


def fetch_nominatim(city: str, country_name: str) -> tuple[float, float] | None:
    """Return (lat, lng) for the best Nominatim result, or None on failure."""
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": f"{city}, {country_name}", "format": "json", "limit": 1},
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if not results:
            return None
        r = results[0]
        place_type = r.get("type", "")
        if place_type not in VALID_TYPES:
            print(f"  [warn] {city}, {country_name}: type={place_type!r}")
        return float(r["lat"]), float(r["lon"])
    except Exception as e:
        print(f"  [error] {city}, {country_name}: {e}")
        return None


def get_unique_pairs(conn, overwrite: bool) -> list[tuple[str, str]]:
    cur = conn.cursor()
    if overwrite:
        cur.execute("""
            SELECT DISTINCT location_city, location_country
            FROM prosopography.organizations
            WHERE location_city IS NOT NULL AND location_country IS NOT NULL
            ORDER BY location_city
        """)
    else:
        cur.execute("""
            SELECT DISTINCT location_city, location_country
            FROM prosopography.organizations
            WHERE location_city IS NOT NULL
              AND location_country IS NOT NULL
              AND location_lat IS NULL
            ORDER BY location_city
        """)
    pairs = cur.fetchall()
    cur.close()
    return [(r[0], r[1]) for r in pairs]


def backfill_orgs(conn, city: str, country: str, lat: float, lng: float) -> int:
    cur = conn.cursor()
    cur.execute("""
        UPDATE prosopography.organizations
           SET location_lat = %(lat)s,
               location_lng = %(lng)s
         WHERE location_city = %(city)s
           AND location_country = %(country)s
    """, {"lat": lat, "lng": lng, "city": city, "country": country})
    updated = cur.rowcount
    conn.commit()
    cur.close()
    return updated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--limit", type=int, default=None, help="Max pairs to process")
    parser.add_argument("--overwrite", action="store_true", help="Re-geocode already-filled orgs")
    args = parser.parse_args()

    conn = get_connection()
    try:
        pairs = get_unique_pairs(conn, overwrite=args.overwrite)
        if args.limit:
            pairs = pairs[: args.limit]

        # Pre-filter pairs with unmappable country codes
        valid_pairs = []
        skipped = 0
        for city, country_code in pairs:
            country_name = resolve_country_name(country_code)
            if country_name is None:
                skipped += 1
                continue
            valid_pairs.append((city, country_code, country_name))

        print(f"[geocode] {len(valid_pairs)} valid pairs (skipped {skipped} unmappable codes)")
        if args.dry_run:
            print("[geocode] DRY RUN — no DB writes")

        ok, failed = 0, 0
        for i, (city, country_code, country_name) in enumerate(valid_pairs, 1):
            coords = fetch_nominatim(city, country_name)
            if coords is None:
                print(f"  [{i}/{len(valid_pairs)}] FAIL  {city}, {country_name}")
                failed += 1
            else:
                lat, lng = coords
                if not args.dry_run:
                    n = backfill_orgs(conn, city, country_code, lat, lng)
                    print(f"  [{i}/{len(valid_pairs)}] OK    {city}, {country_name} ->({lat:.4f}, {lng:.4f})  [{n} orgs]")
                else:
                    print(f"  [{i}/{len(valid_pairs)}] DRY   {city}, {country_name} ->({lat:.4f}, {lng:.4f})")
                ok += 1
            time.sleep(SLEEP_SEC)

        print(f"\n[geocode] Done. Success: {ok}  Failed: {failed}  Skipped: {skipped}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

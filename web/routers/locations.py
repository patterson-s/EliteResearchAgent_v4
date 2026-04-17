from typing import Optional
from fastapi import APIRouter, Query
from web.db import get_conn, rows_to_dicts
from web.models import LocationItem, LocationSummaryResponse

router = APIRouter()


@router.get("/summary", response_model=LocationSummaryResponse)
def locations_summary(level: str = Query("city", pattern="^(city|country)$")):
    with get_conn() as conn:
        cur = conn.cursor()

        if level == "city":
            cur.execute("""
                SELECT
                    o.location_city                                                    AS city,
                    o.location_country                                                 AS country,
                    o.location_region                                                  AS region,
                    MAX(o.location_lat)                                                AS lat,
                    MAX(o.location_lng)                                                AS lng,
                    SUM(1 + GREATEST(0, COALESCE(cp.time_finish - cp.time_start, 0))) AS location_score,
                    COUNT(DISTINCT cp.person_id)                                       AS person_count,
                    COUNT(cp.position_id)                                              AS position_count
                FROM prosopography.organizations o
                JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
                WHERE o.location_city    IS NOT NULL
                  AND o.location_country IS NOT NULL
                  AND o.location_lat     IS NOT NULL
                GROUP BY o.location_city, o.location_country, o.location_region
                ORDER BY location_score DESC
            """)
        else:
            cur.execute("""
                SELECT
                    o.location_country                                                 AS country,
                    SUM(1 + GREATEST(0, COALESCE(cp.time_finish - cp.time_start, 0))) AS location_score,
                    COUNT(DISTINCT cp.person_id)                                       AS person_count,
                    COUNT(cp.position_id)                                              AS position_count
                FROM prosopography.organizations o
                JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
                WHERE o.location_country IS NOT NULL
                GROUP BY o.location_country
                ORDER BY location_score DESC
            """)

        rows = rows_to_dicts(cur)
        cur.close()

    if level == "city":
        items = [
            LocationItem(
                city=r["city"],
                country=r["country"],
                region=r["region"],
                lat=r["lat"],
                lng=r["lng"],
                location_score=int(r["location_score"]),
                person_count=int(r["person_count"]),
                position_count=int(r["position_count"]),
            )
            for r in rows
        ]
    else:
        items = [
            LocationItem(
                country=r["country"],
                location_score=int(r["location_score"]),
                person_count=int(r["person_count"]),
                position_count=int(r["position_count"]),
            )
            for r in rows
        ]

    return LocationSummaryResponse(
        level=level,
        total_locations=len(items),
        total_score=sum(i.location_score for i in items),
        items=items,
    )

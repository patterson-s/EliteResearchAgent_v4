from typing import Optional
from fastapi import APIRouter, Query
from web.db import get_conn, rows_to_dicts
from web.models import (
    LocationItem, LocationSummaryResponse,
    EducationLocationItem, EducationLocationResponse,
    TrajectoryPositionItem, TrajectoryResponse,
)

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


@router.get("/education", response_model=EducationLocationResponse)
def locations_education():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                institution_country,
                COUNT(DISTINCT person_id) AS person_count,
                COUNT(education_id)       AS record_count
            FROM prosopography.education
            WHERE institution_country IS NOT NULL
            GROUP BY institution_country
            ORDER BY person_count DESC, record_count DESC
        """)
        rows = rows_to_dicts(cur)
        cur.close()

    items = [
        EducationLocationItem(
            institution_country=r["institution_country"],
            person_count=int(r["person_count"]),
            record_count=int(r["record_count"]),
        )
        for r in rows
    ]
    total_persons = sum(i.person_count for i in items)
    return EducationLocationResponse(
        total_locations=len(items),
        total_persons=total_persons,
        items=items,
    )


@router.get("/trajectory/{person_id}", response_model=TrajectoryResponse)
def locations_trajectory(person_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                cp.position_id,
                cp.title,
                cp.organization,
                cp.time_start,
                cp.time_finish,
                o.canonical_name  AS org_canonical_name,
                o.location_city   AS city,
                o.location_country AS country,
                o.location_lat    AS lat,
                o.location_lng    AS lng
            FROM prosopography.career_positions cp
            JOIN prosopography.organizations o ON o.org_id = cp.org_id
            WHERE cp.person_id = %s
              AND o.location_lat IS NOT NULL
              AND o.location_lng IS NOT NULL
            ORDER BY cp.time_start NULLS LAST, cp.sort_order
        """, (person_id,))
        rows = rows_to_dicts(cur)
        cur.close()

    positions = [
        TrajectoryPositionItem(
            position_id=int(r["position_id"]),
            title=r["title"],
            organization=r["organization"],
            org_canonical_name=r["org_canonical_name"],
            city=r["city"],
            country=r["country"],
            lat=float(r["lat"]),
            lng=float(r["lng"]),
            time_start=r["time_start"],
            time_finish=r["time_finish"],
        )
        for r in rows
    ]
    return TrajectoryResponse(person_id=person_id, positions=positions)

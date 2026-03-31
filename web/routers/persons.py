from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from web.db import get_conn, rows_to_dicts
from web.models import (
    PersonListResponse, PersonListItem, PersonFilterMeta, PersonDetail,
    CareerPositionItem, EducationItem, AwardItem, HLPItem,
    PersonAttributeItem, PositionTagItem,
)

router = APIRouter()


@router.get("/filters/meta", response_model=PersonFilterMeta)
def person_filter_meta():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT h.hlp_id, h.hlp_name, h.hlp_year, h.un_sg,
                   COUNT(p.person_id) AS member_count
            FROM prosopography.hlp_panels h
            LEFT JOIN prosopography.persons p ON p.hlp_id = h.hlp_id
            GROUP BY h.hlp_id, h.hlp_name, h.hlp_year, h.un_sg
            ORDER BY h.hlp_year
        """)
        panels = [HLPItem(**r) for r in rows_to_dicts(cur)]

        cur.execute("""
            SELECT DISTINCT nationality FROM prosopography.person_nationalities
            ORDER BY nationality
        """)
        nationalities = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT (birth_year / 10) * 10 AS decade
            FROM prosopography.persons
            WHERE birth_year IS NOT NULL
            ORDER BY decade
        """)
        decades = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT pa.attribute_value
            FROM prosopography.person_attributes pa
            WHERE pa.attribute_name = 'career_domain'
            ORDER BY pa.attribute_value
        """)
        career_domains = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT pa.attribute_value
            FROM prosopography.person_attributes pa
            WHERE pa.attribute_name = 'career_typology'
            ORDER BY pa.attribute_value
        """)
        career_typologies = [r[0] for r in cur.fetchall()]
        cur.close()

    return PersonFilterMeta(
        hlp_panels=panels,
        nationalities=nationalities,
        birth_decades=decades,
        career_domains=career_domains,
        career_typologies=career_typologies,
    )


@router.get("", response_model=PersonListResponse)
def list_persons(
    hlp_id: Optional[int] = Query(None),
    nationality: Optional[str] = Query(None),
    birth_decade: Optional[int] = Query(None),
    career_domain: Optional[str] = Query(None),
    career_typology: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    limit: int = Query(100, le=200),
    offset: int = Query(0),
):
    # Build WHERE clauses dynamically to avoid psycopg2 NULL casting issues
    where_parts = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}

    if hlp_id is not None:
        where_parts.append("p.hlp_id = %(hlp_id)s")
        params["hlp_id"] = hlp_id
    if nationality is not None:
        where_parts.append("""p.person_id IN (
            SELECT person_id FROM prosopography.person_nationalities
            WHERE nationality ILIKE %(nationality)s)""")
        params["nationality"] = nationality
    if birth_decade is not None:
        where_parts.append("p.birth_year >= %(decade_lo)s AND p.birth_year < %(decade_hi)s")
        params["decade_lo"] = birth_decade
        params["decade_hi"] = birth_decade + 10
    if career_domain is not None:
        where_parts.append("""p.person_id IN (
            SELECT person_id FROM prosopography.person_attributes
            WHERE attribute_name = 'career_domain' AND attribute_value = %(career_domain)s)""")
        params["career_domain"] = career_domain
    if career_typology is not None:
        where_parts.append("""p.person_id IN (
            SELECT person_id FROM prosopography.person_attributes
            WHERE attribute_name = 'career_typology' AND attribute_value = %(career_typology)s)""")
        params["career_typology"] = career_typology
    if q is not None:
        where_parts.append("p.display_name ILIKE '%%' || %(q)s || '%%'")
        params["q"] = q

    where_sql = " AND ".join(where_parts)

    list_sql = f"""
        SELECT
            p.person_id, p.display_name, p.birth_year, p.death_status,
            p.hlp_id, h.hlp_name,
            nat.nationalities,
            COUNT(DISTINCT cp.position_id) AS position_count
        FROM prosopography.persons p
        JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(pn.nationality ORDER BY pn.sort_order) AS nationalities
            FROM prosopography.person_nationalities pn
            WHERE pn.person_id = p.person_id
        ) nat ON true
        LEFT JOIN prosopography.career_positions cp ON cp.person_id = p.person_id
        WHERE {where_sql}
        GROUP BY p.person_id, p.display_name, p.birth_year, p.death_status,
                 p.hlp_id, h.hlp_name, nat.nationalities
        ORDER BY p.display_name
        LIMIT %(limit)s OFFSET %(offset)s
    """
    count_sql = f"""
        SELECT COUNT(DISTINCT p.person_id)
        FROM prosopography.persons p
        JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
        LEFT JOIN prosopography.person_nationalities pn ON pn.person_id = p.person_id
        WHERE {where_sql}
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(count_sql, params)
        total = cur.fetchone()[0]
        cur.execute(list_sql, params)
        rows = rows_to_dicts(cur)
        cur.close()

    items = [
        PersonListItem(
            person_id=r["person_id"],
            display_name=r["display_name"],
            birth_year=r["birth_year"],
            death_status=r["death_status"],
            hlp_id=r["hlp_id"],
            hlp_name=r["hlp_name"],
            nationalities=r["nationalities"] or [],
            position_count=r["position_count"],
        )
        for r in rows
    ]
    return PersonListResponse(total=total, items=items)


@router.get("/{person_id}", response_model=PersonDetail)
def get_person(person_id: int):
    with get_conn() as conn:
        cur = conn.cursor()

        # 1. Person base row
        cur.execute("""
            SELECT p.person_id, p.display_name, p.birth_year, p.death_status, p.death_year,
                   p.hlp_id, p.hlp_nomination_age, h.hlp_name, h.hlp_year,
                   ARRAY_AGG(pn.nationality ORDER BY pn.sort_order)
                       FILTER (WHERE pn.nationality IS NOT NULL) AS nationalities
            FROM prosopography.persons p
            JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
            LEFT JOIN prosopography.person_nationalities pn ON pn.person_id = p.person_id
            WHERE p.person_id = %(pid)s
            GROUP BY p.person_id, p.display_name, p.birth_year, p.death_status, p.death_year,
                     p.hlp_id, p.hlp_nomination_age, h.hlp_name, h.hlp_year
        """, {"pid": person_id})
        person_row = cur.fetchone()
        if not person_row:
            raise HTTPException(status_code=404, detail="Person not found")
        from web.db import row_to_dict
        p = row_to_dict(cur, person_row)

        # 2. Career positions + embedded tags (single join, no N+1)
        cur.execute("""
            SELECT cp.position_id, cp.title, cp.organization, cp.org_id,
                   o.canonical_name AS org_canonical_name,
                   cp.time_start, cp.time_finish, cp.approximate_period,
                   cp.role_type, cp.confidence, cp.event_source, cp.sort_order,
                   pt.domain, pt.organization_type, pt.un_placement,
                   pt.geographic_scope,
                   pt.role_type      AS tag_role_type,
                   pt.function,      pt.career_phase,  pt.policy_bridge
            FROM prosopography.career_positions cp
            LEFT JOIN prosopography.organizations o ON o.org_id = cp.org_id
            LEFT JOIN prosopography.position_tags pt ON pt.position_id = cp.position_id
            WHERE cp.person_id = %(pid)s
            ORDER BY cp.time_start NULLS LAST, cp.sort_order
        """, {"pid": person_id})
        pos_rows = rows_to_dicts(cur)
        positions = []
        for r in pos_rows:
            has_tags = r.get("career_phase") is not None or r.get("domain") is not None
            tags = PositionTagItem(
                domain=r["domain"],
                organization_type=r["organization_type"],
                un_placement=r["un_placement"],
                geographic_scope=r["geographic_scope"],
                role_type=r["tag_role_type"],
                function=r["function"],
                career_phase=r["career_phase"],
                policy_bridge=r["policy_bridge"],
            ) if has_tags else None
            positions.append(CareerPositionItem(
                position_id=r["position_id"],
                title=r["title"],
                organization=r["organization"],
                org_id=r["org_id"],
                org_canonical_name=r["org_canonical_name"],
                time_start=r["time_start"],
                time_finish=r["time_finish"],
                approximate_period=r["approximate_period"],
                role_type=r["role_type"],
                confidence=r["confidence"],
                event_source=r["event_source"],
                sort_order=r["sort_order"],
                tags=tags,
            ))

        # 3. Education
        cur.execute("""
            SELECT education_id, degree_name, degree_type, field, institution,
                   institution_country, time_start, time_finish, event_source, sort_order
            FROM prosopography.education
            WHERE person_id = %(pid)s
            ORDER BY time_start NULLS LAST, sort_order
        """, {"pid": person_id})
        education = [EducationItem(**r) for r in rows_to_dicts(cur)]

        # 4. Awards
        cur.execute("""
            SELECT award_id, award_name, awarding_organization, award_type,
                   time_start, confidence, event_source, sort_order
            FROM prosopography.awards
            WHERE person_id = %(pid)s
            ORDER BY time_start NULLS LAST, sort_order
        """, {"pid": person_id})
        awards = [AwardItem(**r) for r in rows_to_dicts(cur)]

        # 5. Person-level attributes
        cur.execute("""
            SELECT pa.attribute_name, pa.attribute_value, pa.attribute_label, pa.confidence
            FROM prosopography.person_attributes pa
            WHERE pa.person_id = %(pid)s AND pa.is_primary = true
            ORDER BY pa.attribute_name
        """, {"pid": person_id})
        attributes = [PersonAttributeItem(**r) for r in rows_to_dicts(cur)]
        cur.close()

    return PersonDetail(
        person_id=p["person_id"],
        display_name=p["display_name"],
        birth_year=p["birth_year"],
        death_status=p["death_status"],
        death_year=p["death_year"],
        hlp_id=p["hlp_id"],
        hlp_name=p["hlp_name"],
        hlp_year=p["hlp_year"],
        hlp_nomination_age=p["hlp_nomination_age"],
        nationalities=p["nationalities"] or [],
        attributes=attributes,
        career_positions=positions,
        education=education,
        awards=awards,
    )

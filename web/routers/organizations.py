from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from web.db import get_conn, rows_to_dicts, row_to_dict
from web.models import (
    OrgListResponse, OrgListItem, OrgFilterMeta,
    OrgDetail, OrgCorpusMember, OrgCorpusMemberRole, OrgTooltip,
)

router = APIRouter()


@router.get("/filters/meta", response_model=OrgFilterMeta)
def org_filter_meta():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT meta_type FROM prosopography.organizations
            WHERE meta_type IS NOT NULL ORDER BY meta_type
        """)
        meta_types = [r[0] for r in cur.fetchall()]
        cur.execute("""
            SELECT DISTINCT sector FROM prosopography.organizations
            WHERE sector IS NOT NULL ORDER BY sector
        """)
        sectors = [r[0] for r in cur.fetchall()]
        cur.close()
    return OrgFilterMeta(meta_types=meta_types, sectors=sectors)


@router.get("", response_model=OrgListResponse)
def list_organizations(
    meta_type: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    limit: int = Query(50, le=2000),
    offset: int = Query(0),
):
    params = {"meta_type": meta_type, "sector": sector, "q": q, "limit": limit, "offset": offset}
    where = """
        (%(meta_type)s IS NULL OR o.meta_type = %(meta_type)s)
        AND (%(sector)s IS NULL OR o.sector = %(sector)s)
        AND (%(q)s IS NULL OR o.canonical_name ILIKE '%%' || %(q)s || '%%')
    """
    list_sql = f"""
        SELECT o.org_id, o.canonical_name, o.meta_type, o.sector,
               o.location_country, o.location_city, o.review_status,
               COUNT(DISTINCT cp.person_id) AS corpus_member_count
        FROM prosopography.organizations o
        LEFT JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
        WHERE {where}
        GROUP BY o.org_id, o.canonical_name, o.meta_type, o.sector,
                 o.location_country, o.location_city, o.review_status
        ORDER BY corpus_member_count DESC, o.canonical_name
        LIMIT %(limit)s OFFSET %(offset)s
    """
    count_sql = f"""
        SELECT COUNT(DISTINCT o.org_id)
        FROM prosopography.organizations o
        WHERE {where}
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(count_sql, params)
        total = cur.fetchone()[0]
        cur.execute(list_sql, params)
        rows = rows_to_dicts(cur)
        cur.close()
    items = [OrgListItem(**r) for r in rows]
    return OrgListResponse(total=total, items=items)


@router.get("/{org_id}/tooltip", response_model=OrgTooltip)
def org_tooltip(org_id: int):
    sql = """
        SELECT o.org_id, o.canonical_name, o.meta_type, o.sector,
               o.location_country, o.location_city,
               o.un_hierarchical_tags, o.gov_hierarchical_tags,
               COUNT(DISTINCT cp.person_id) AS corpus_member_count
        FROM prosopography.organizations o
        LEFT JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
        WHERE o.org_id = %(org_id)s
        GROUP BY o.org_id, o.canonical_name, o.meta_type, o.sector,
                 o.location_country, o.location_city, o.un_hierarchical_tags, o.gov_hierarchical_tags
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"org_id": org_id})
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Organization not found")
        r = row_to_dict(cur, row)
        cur.close()
    return OrgTooltip(**r)


@router.get("/{org_id}", response_model=OrgDetail)
def get_org(org_id: int):
    with get_conn() as conn:
        cur = conn.cursor()

        # 1. Org base row + aliases
        cur.execute("""
            SELECT o.org_id, o.canonical_name, o.meta_type, o.org_types, o.sector,
                   o.location_country, o.location_city,
                   o.un_canonical_tag, o.un_hierarchical_tags,
                   o.gov_canonical_tag, o.gov_hierarchical_tags, o.review_status,
                   ARRAY_AGG(DISTINCT oa.alias) FILTER (WHERE oa.alias IS NOT NULL) AS aliases
            FROM prosopography.organizations o
            LEFT JOIN prosopography.organization_aliases oa ON oa.org_id = o.org_id
            WHERE o.org_id = %(org_id)s
            GROUP BY o.org_id, o.canonical_name, o.meta_type, o.org_types, o.sector,
                     o.location_country, o.location_city, o.un_canonical_tag, o.un_hierarchical_tags,
                     o.gov_canonical_tag, o.gov_hierarchical_tags, o.review_status
        """, {"org_id": org_id})
        org_row = cur.fetchone()
        if not org_row:
            raise HTTPException(status_code=404, detail="Organization not found")
        o = row_to_dict(cur, org_row)

        # 2. Corpus members + roles
        cur.execute("""
            SELECT p.person_id, p.display_name, p.hlp_id, h.hlp_name,
                   cp.title, cp.time_start, cp.time_finish, cp.role_type
            FROM prosopography.career_positions cp
            JOIN prosopography.persons p ON p.person_id = cp.person_id
            JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
            WHERE cp.org_id = %(org_id)s
            ORDER BY p.display_name, cp.time_start NULLS LAST
        """, {"org_id": org_id})
        member_rows = rows_to_dicts(cur)
        cur.close()

    # Group roles by person
    members_map: dict[int, OrgCorpusMember] = {}
    for r in member_rows:
        pid = r["person_id"]
        if pid not in members_map:
            members_map[pid] = OrgCorpusMember(
                person_id=pid,
                display_name=r["display_name"],
                hlp_id=r["hlp_id"],
                hlp_name=r["hlp_name"],
                roles=[],
            )
        members_map[pid].roles.append(OrgCorpusMemberRole(
            title=r["title"],
            time_start=r["time_start"],
            time_finish=r["time_finish"],
            role_type=r["role_type"],
        ))

    return OrgDetail(
        org_id=o["org_id"],
        canonical_name=o["canonical_name"],
        meta_type=o["meta_type"],
        org_types=o["org_types"],
        sector=o["sector"],
        location_country=o["location_country"],
        location_city=o["location_city"],
        un_canonical_tag=o["un_canonical_tag"],
        un_hierarchical_tags=o["un_hierarchical_tags"],
        gov_canonical_tag=o["gov_canonical_tag"],
        gov_hierarchical_tags=o["gov_hierarchical_tags"],
        review_status=o["review_status"],
        aliases=o["aliases"] or [],
        corpus_members=list(members_map.values()),
    )

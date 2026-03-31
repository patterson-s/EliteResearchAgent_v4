from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from web.db import get_conn, rows_to_dicts
from web.models import SearchResponse, SearchResultItem

router = APIRouter()


@router.get("", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(12, le=50),
):
    sql = """
        SELECT 'person' AS type, p.person_id AS id,
               p.display_name AS label, h.hlp_name AS sublabel
        FROM prosopography.persons p
        JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
        WHERE p.display_name ILIKE '%%' || %(q)s || '%%'
        UNION ALL
        SELECT 'organization' AS type, o.org_id AS id,
               o.canonical_name AS label, o.meta_type AS sublabel
        FROM prosopography.organizations o
        WHERE o.canonical_name ILIKE '%%' || %(q)s || '%%'
          AND o.review_status != 'pending_review'
        ORDER BY label
        LIMIT %(limit)s
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"q": q, "limit": limit})
        rows = rows_to_dicts(cur)
        cur.close()
    results = [SearchResultItem(**r) for r in rows]
    return SearchResponse(query=q, results=results)

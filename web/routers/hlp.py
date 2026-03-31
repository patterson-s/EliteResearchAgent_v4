from fastapi import APIRouter
from web.db import get_conn, rows_to_dicts
from web.models import HLPItem

router = APIRouter()


@router.get("", response_model=list[HLPItem])
def list_hlp_panels():
    sql = """
        SELECT h.hlp_id, h.hlp_name, h.hlp_year, h.un_sg,
               COUNT(p.person_id) AS member_count
        FROM prosopography.hlp_panels h
        LEFT JOIN prosopography.persons p ON p.hlp_id = h.hlp_id
        GROUP BY h.hlp_id, h.hlp_name, h.hlp_year, h.un_sg
        ORDER BY h.hlp_year
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = rows_to_dicts(cur)
        cur.close()
    return [HLPItem(**r) for r in rows]

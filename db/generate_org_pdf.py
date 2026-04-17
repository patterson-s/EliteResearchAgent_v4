"""
Generate a single PDF covering all corpus-linked organizations.

Output: static/org_pdfs/organizations.pdf

Run from the project root:
    python db/generate_org_pdf.py
"""

import html
import sys
from collections import defaultdict
from datetime import date
from io import BytesIO
from pathlib import Path

import psycopg2.extras
from xhtml2pdf import pisa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_utils import get_connection

OUTPUT_DIR = Path(__file__).parent.parent / "static" / "org_pdfs"
OUTPUT_FILE = OUTPUT_DIR / "organizations.pdf"

# ── String helpers ─────────────────────────────────────────────────────────────

def esc(val) -> str:
    if val is None:
        return ""
    return html.escape(str(val))


def year_range(pos: dict) -> str:
    if pos.get("approximate_period"):
        return esc(pos["approximate_period"])
    start = pos.get("time_start")
    finish = pos.get("time_finish")
    if start and finish:
        return f"{start}\u2013{finish}"
    if start:
        return f"{start}\u2013"
    if finish:
        return f"\u2013{finish}"
    return "n.d."


def badge(text: str, bg: str = "#555", fg: str = "#fff") -> str:
    return (
        f'<span style="background:{bg};color:{fg};padding:1px 5px;'
        f'border-radius:3px;font-size:7.5pt;white-space:nowrap;">{esc(text)}</span> '
    )


def section_wrap(title: str, content: str) -> str:
    if not content.strip():
        return ""
    return (
        '<div style="margin-top:14px;">'
        f'<h2 style="font-size:10pt;text-transform:uppercase;letter-spacing:.04em;'
        f'color:#333;border-bottom:1px solid #ccc;margin:0 0 5px;padding-bottom:2px;">'
        f'{title}</h2>'
        + content +
        "</div>"
    )


# ── Section renderers ──────────────────────────────────────────────────────────

ROLE_COLORS = {
    "primary":    "#c0392b",
    "advisory":   "#2980b9",
    "governance": "#27ae60",
}
META_TYPE_COLORS = {
    "national_government": "#1a6fa3",
    "international_organization": "#2e7d32",
    "ngo": "#6a1a8a",
    "private_sector": "#795548",
    "academic": "#e65100",
    "media": "#37474f",
}


def render_positions_table(positions: list[dict]) -> str:
    if not positions:
        return '<p style="font-size:8.5pt;color:#888;font-style:italic;">No corpus members at this organization.</p>'
    rows = []
    for pos in positions:
        yr = year_range(pos)
        role = pos.get("role_type")
        role_badge = badge(role.capitalize(), ROLE_COLORS.get(role, "#555")) if role else ""
        hlp_label = f"HLP {esc(str(pos.get('hlp_year', '')))} \u00b7 {esc(pos.get('hlp_name', ''))}"
        rows.append(
            '<tr style="page-break-inside:avoid;border-bottom:1px solid #eee;">'
            f'<td style="padding:3px 6px;font-size:9pt;font-weight:bold;vertical-align:top;">{esc(pos.get("display_name",""))}</td>'
            f'<td style="padding:3px 6px;font-size:8pt;color:#555;vertical-align:top;">{hlp_label}</td>'
            f'<td style="padding:3px 6px;font-size:8.5pt;vertical-align:top;">{esc(pos.get("title",""))}{(" " + role_badge) if role_badge else ""}</td>'
            f'<td style="padding:3px 6px;font-size:8pt;color:#666;vertical-align:top;white-space:nowrap;">{yr}</td>'
            "</tr>"
        )
    return (
        '<table width="100%" style="border-collapse:collapse;" cellpadding="0" cellspacing="0">'
        '<tr style="background:#f5f5f5;">'
        '<th style="padding:3px 6px;font-size:8pt;text-align:left;">Person</th>'
        '<th style="padding:3px 6px;font-size:8pt;text-align:left;">Panel</th>'
        '<th style="padding:3px 6px;font-size:8pt;text-align:left;">Title</th>'
        '<th style="padding:3px 6px;font-size:8pt;text-align:left;">Years</th>'
        "</tr>"
        + "\n".join(rows)
        + "</table>"
    )


def render_ontology_block(ont: dict) -> str:
    parts = []
    eq = ont.get("display_label") or ont.get("equivalence_class") or ""
    if eq:
        parts.append(f'<p style="margin:2px 0;font-size:8.5pt;"><b>Equivalence Class:</b> {esc(eq)}</p>')
    cat = ont.get("ontology_category") or ont.get("parent_category") or ""
    if cat:
        parts.append(f'<p style="margin:2px 0;font-size:8.5pt;"><b>Category:</b> {esc(cat)}</p>')
    cc = ont.get("country_code") or ""
    if cc:
        parts.append(f'<p style="margin:2px 0;font-size:8.5pt;"><b>Country:</b> {esc(cc)}</p>')
    hier = ont.get("hierarchy_path") or []
    if hier:
        arrow = " \u203a "
        parts.append(
            f'<p style="margin:2px 0;font-size:8.5pt;"><b>Hierarchy:</b> {arrow.join(esc(x) for x in hier)}</p>'
        )
    tags = ont.get("thematic_tags") or []
    if tags:
        parts.append(
            f'<p style="margin:2px 0;font-size:8.5pt;"><b>Thematic Tags:</b> {esc(", ".join(tags))}</p>'
        )
    return "\n".join(parts)


def render_org_section(org: dict, positions: list[dict], ont: dict | None, first: bool) -> str:
    top_margin = "margin-top:0;" if first else "margin-top:14px;border-top:2px solid #2c3e50;padding-top:8px;"

    meta_parts = []
    if org.get("meta_type"):
        meta_parts.append(esc(org["meta_type"].replace("_", " ").title()))
    if org.get("sector"):
        meta_parts.append(esc(org["sector"]))
    if org.get("location_country"):
        loc = org["location_country"]
        if org.get("location_city"):
            loc = f"{org['location_city']}, {loc}"
        meta_parts.append(esc(loc))
    meta_line = " \u00b7 ".join(meta_parts)

    person_count = org.get("person_count", 0)
    position_count = org.get("position_count", 0)
    count_str = f"{person_count} panel member{'s' if person_count != 1 else ''}, {position_count} position{'s' if position_count != 1 else ''}"

    aliases = org.get("aliases") or []
    aliases_html = ""
    if aliases:
        alias_text = "; ".join(esc(a) for a in sorted(aliases))
        aliases_html = f'<p style="font-size:8pt;color:#666;margin:2px 0;"><i>Also known as: {alias_text}</i></p>'

    stub_note = ""
    if org.get("review_status") == "pending_review":
        stub_note = ' <span style="font-size:7.5pt;color:#999;">(stub \u2014 pending review)</span>'

    positions_html = render_positions_table(positions)
    ont_html = render_ontology_block(ont) if ont else ""

    return f"""
<div style="{top_margin}page-break-inside:avoid;">
  <div style="background:#2c3e50;padding:4px 8px;margin-bottom:3px;">
    <span style="font-size:11pt;font-weight:bold;color:#ffffff;">{esc(org.get("canonical_name",""))}{stub_note}</span>
  </div>
  <p style="font-size:8.5pt;color:#555;margin:0 0 2px;">{meta_line} &nbsp;<span style="color:#aaa;">{esc(count_str)}</span></p>
  {aliases_html}
  {section_wrap("Corpus Members", positions_html)}
  {section_wrap("Ontology Classification", ont_html)}
</div>
"""


def render_cover(total_orgs: int, gen_date: str) -> str:
    return f"""
<div style="page-break-after:always;text-align:center;padding-top:80mm;">
  <h1 style="font-size:22pt;margin:0 0 10px;">Organization Directory</h1>
  <p style="font-size:12pt;color:#555;margin:0 0 6px;">Prosopography Database &mdash; UN High-Level Panels</p>
  <p style="font-size:10pt;color:#777;margin:0 0 4px;">Generated: {esc(gen_date)}</p>
  <p style="font-size:10pt;color:#777;margin:0;">{total_orgs} corpus-linked organizations</p>
</div>
"""


# ── HTML assembly ──────────────────────────────────────────────────────────────

CSS = """
@page { size: A4; margin: 15mm 15mm; }
body {
  font-family: Helvetica, Arial, sans-serif;
  font-size: 9pt;
  color: #222;
  line-height: 1.4;
  margin: 0;
  padding: 0;
}
h1 { font-size: 13pt; margin: 0 0 3px; }
h2 { font-size: 10pt; }
table { border-collapse: collapse; width: 100%; }
"""


def build_full_html(
    orgs: list[dict],
    positions_by_org: dict[int, list[dict]],
    ontology_by_org: dict[int, dict],
) -> str:
    gen_date = date.today().isoformat()
    sections = [render_cover(len(orgs), gen_date)]
    for i, org in enumerate(orgs):
        oid = org["org_id"]
        positions = positions_by_org.get(oid, [])
        ont = ontology_by_org.get(oid)
        sections.append(render_org_section(org, positions, ont, first=(i == 0)))

    body = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<style>{CSS}</style>
</head>
<body>
{body}
</body>
</html>"""


# ── DB fetch ───────────────────────────────────────────────────────────────────

def fetch_all_data(conn) -> tuple[list[dict], dict[int, list[dict]], dict[int, dict]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            o.org_id,
            o.canonical_name,
            o.meta_type,
            o.sector,
            o.location_country,
            o.location_city,
            o.review_status,
            ARRAY_AGG(DISTINCT oa.alias) FILTER (WHERE oa.alias IS NOT NULL) AS aliases,
            COUNT(DISTINCT cp.person_id) AS person_count,
            COUNT(cp.position_id)        AS position_count
        FROM prosopography.organizations o
        JOIN prosopography.career_positions cp ON cp.org_id = o.org_id
        LEFT JOIN prosopography.organization_aliases oa ON oa.org_id = o.org_id
        GROUP BY o.org_id, o.canonical_name, o.meta_type, o.sector,
                 o.location_country, o.location_city, o.review_status
        ORDER BY person_count DESC, position_count DESC, o.canonical_name
    """)
    orgs = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT
            cp.org_id,
            p.person_id,
            p.display_name,
            cp.title,
            cp.time_start,
            cp.time_finish,
            cp.approximate_period,
            cp.role_type,
            h.hlp_name,
            h.hlp_year
        FROM prosopography.career_positions cp
        JOIN prosopography.persons p ON p.person_id = cp.person_id
        JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
        WHERE cp.org_id IS NOT NULL
        ORDER BY cp.org_id, p.display_name, cp.time_start NULLS LAST
    """)
    positions_by_org: dict[int, list[dict]] = defaultdict(list)
    for row in cur.fetchall():
        positions_by_org[row["org_id"]].append(dict(row))

    cur.execute("""
        SELECT DISTINCT ON (m.org_id)
            m.org_id,
            m.equivalence_class,
            m.country_code,
            m.hierarchy_path,
            m.display_label,
            m.thematic_tags,
            m.parent_category,
            r.scope_json->>'category' AS ontology_category
        FROM prosopography.org_ontology_mappings m
        JOIN prosopography.derivative_runs r ON r.run_id = m.run_id
        WHERE r.run_id IN (5, 6, 8)
          AND m.equivalence_class NOT LIKE 'not\\_%'
          AND m.equivalence_class NOT IN ('needs_review', 'presidential_campaign')
        ORDER BY m.org_id, m.run_id DESC
    """)
    ontology_by_org: dict[int, dict] = {row["org_id"]: dict(row) for row in cur.fetchall()}

    cur.close()
    return orgs, dict(positions_by_org), ontology_by_org


# ── PDF writer ─────────────────────────────────────────────────────────────────

def write_pdf(html_str: str, out_path: Path) -> None:
    buf = BytesIO()
    status = pisa.CreatePDF(html_str.encode("utf-8"), dest=buf, encoding="utf-8")
    if status.err:
        raise RuntimeError(f"xhtml2pdf error code {status.err}")
    out_path.write_bytes(buf.getvalue())


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    try:
        print("Fetching data...")
        orgs, positions_by_org, ontology_by_org = fetch_all_data(conn)
        print(f"  {len(orgs)} corpus-linked organizations")
        print(f"  {sum(len(v) for v in positions_by_org.values())} positions")
        print(f"  {len(ontology_by_org)} ontology mappings")

        print("Building HTML...")
        html_str = build_full_html(orgs, positions_by_org, ontology_by_org)

        print(f"Rendering PDF -> {OUTPUT_FILE}")
        write_pdf(html_str, OUTPUT_FILE)
        size_kb = OUTPUT_FILE.stat().st_size // 1024
        print(f"Done. ({size_kb} KB)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

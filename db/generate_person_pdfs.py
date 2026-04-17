"""
Generate one PDF per person from the prosopography database.

Output: static/person_pdfs/{person_id:03d}_{slug}.pdf

Run from the project root:
    python db/generate_person_pdfs.py
"""

import html
import os
import re
import sys
from io import BytesIO
from pathlib import Path

import psycopg2.extras
from xhtml2pdf import pisa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.db_utils import get_connection

OUTPUT_DIR = Path(__file__).parent.parent / "static" / "person_pdfs"

# ── String helpers ─────────────────────────────────────────────────────────────

def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


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


# ── Badge / chip builders ──────────────────────────────────────────────────────

def badge(text: str, bg: str = "#555", fg: str = "#fff") -> str:
    return (
        f'<span style="background:{bg};color:{fg};padding:1px 5px;'
        f'border-radius:3px;font-size:7.5pt;white-space:nowrap;">{esc(text)}</span> '
    )


def outline_chip(label: str, value: str, color: str) -> str:
    return (
        f'<span style="border:1px solid {color};color:{color};padding:1px 6px;'
        f'border-radius:4px;font-size:8pt;margin-right:4px;">'
        f'<b>{esc(label)}:</b> {esc(value)}</span> '
    )


ROLE_COLORS = {
    "primary":    "#c0392b",
    "advisory":   "#2980b9",
    "governance": "#27ae60",
}
PHASE_LABELS = {
    "formative":     "Formative",
    "consolidation": "Consolidation",
    "apex":          "Apex",
    "post_apex":     "Post-Apex",
}
PHASE_COLORS = {
    "formative":     "#8e44ad",
    "consolidation": "#2471a3",
    "apex":          "#d35400",
    "post_apex":     "#7f8c8d",
}
CHIP_META = [
    ("career_domain",        "Domain",   "#1a6fa3"),
    ("career_typology",      "Typology", "#2e7d32"),
    ("functional_summary",   "Mobility", "#6a1a8a"),
    ("institution_prestige", "Prestige", "#795548"),
    ("geo_edu_category",     "Edu-Geo",  "#37474f"),
]
MOBILITY_TYPE_COLORS = {
    "monofunctional":  "#1565c0",
    "bifunctional":    "#2e7d32",
    "multifunctional": "#6a1a8a",
    "transitional":    "#e65100",
}

# ── Section wrapper ────────────────────────────────────────────────────────────

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

# ── Career section ─────────────────────────────────────────────────────────────

def render_career(positions: list[dict]) -> str:
    if not positions:
        return ""
    rows = []
    for pos in positions:
        yr = year_range(pos)
        org_name = esc(pos.get("org_canonical_name") or pos.get("organization") or "")
        org_html = f'<div style="font-size:8.5pt;color:#444;margin-top:1px;">{org_name}</div>' if org_name else ""

        badges: list[str] = []
        role = pos.get("role_type")
        if role:
            badges.append(badge(role.capitalize(), ROLE_COLORS.get(role, "#555")))
        phase = pos.get("career_phase")
        if phase:
            badges.append(badge(PHASE_LABELS.get(phase, phase), PHASE_COLORS.get(phase, "#555")))
        domains = pos.get("domain") or []
        if domains:
            badges.append(badge(domains[0], "#00695c"))
            if len(domains) > 1:
                badges.append(badge(f"+{len(domains) - 1}", "#00897b"))
        src = pos.get("event_source")
        if src:
            badges.append(badge(src, "#78909c"))
        badge_row = (
            f'<div style="margin-top:2px;">{"".join(badges)}</div>'
            if badges else ""
        )

        ftags = pos.get("functional_tags") or []
        ftag_html = ""
        if ftags:
            spans = "".join(
                f'<span style="border:1px solid #aaa;border-radius:3px;'
                f'padding:0 4px;margin-right:3px;font-size:7.5pt;">{esc(t)}</span>'
                for t in ftags
            )
            ftag_html = f'<div style="margin-top:2px;color:#555;">{spans}</div>'

        rows.append(
            '<table width="100%" style="border-bottom:1px solid #eee;margin-bottom:3px;'
            'page-break-inside:avoid;" cellpadding="0" cellspacing="0"><tr>'
            f'<td width="70" style="font-size:8pt;color:#666;vertical-align:top;'
            f'text-align:right;padding:4px 6px 4px 0;">{yr}</td>'
            '<td style="vertical-align:top;padding:4px 0;">'
            f'<div style="font-weight:bold;font-size:9pt;">{esc(pos.get("title",""))}</div>'
            + org_html + badge_row + ftag_html +
            "</td></tr></table>"
        )
    return "\n".join(rows)


# ── Education section ──────────────────────────────────────────────────────────

def render_education(education: list[dict]) -> str:
    if not education:
        return ""
    rows = []
    for e in education:
        if e.get("degree_name"):
            degree = esc(e["degree_name"])
        elif e.get("degree_type") and e.get("field"):
            degree = f"{esc(e['degree_type'])} in {esc(e['field'])}"
        elif e.get("degree_type"):
            degree = esc(e["degree_type"])
        else:
            degree = "Degree"

        inst_parts = []
        if e.get("institution"):
            inst_parts.append(esc(e["institution"]))
        if e.get("institution_country"):
            inst_parts.append(f"({esc(e['institution_country'])})")
        inst_str = " ".join(inst_parts)

        yr_parts = []
        if e.get("time_start"):
            yr_parts.append(str(e["time_start"]))
        if e.get("time_finish"):
            yr_parts.append(str(e["time_finish"]))
        yr_str = "\u2013".join(yr_parts)

        inst_html = f' <span style="font-size:8.5pt;color:#444;">\u00b7 {inst_str}</span>' if inst_str else ""
        yr_html = f' <span style="font-size:8pt;color:#777;">{yr_str}</span>' if yr_str else ""

        rows.append(
            '<div style="border-bottom:1px solid #eee;padding:4px 0;page-break-inside:avoid;">'
            f'<b style="font-size:9pt;">{degree}</b>{inst_html}{yr_html}'
            "</div>"
        )
    return "\n".join(rows)


# ── Awards section ─────────────────────────────────────────────────────────────

def render_awards(awards: list[dict]) -> str:
    if not awards:
        return ""
    by_decade: dict[str, list] = {}
    for aw in awards:
        yr = aw.get("time_start")
        decade = f"{(yr // 10) * 10}s" if yr else "Undated"
        by_decade.setdefault(decade, []).append(aw)

    parts = []
    for decade in sorted(by_decade.keys()):
        parts.append(
            f'<div style="font-weight:bold;font-size:8.5pt;color:#444;'
            f'margin:6px 0 2px;">{esc(decade)}</div>'
        )
        for aw in by_decade[decade]:
            yr_str = str(aw["time_start"]) if aw.get("time_start") else ""
            org_str = esc(aw.get("awarding_organization") or "")
            atype = esc(aw.get("award_type") or "")

            org_html = f' <span style="font-size:8.5pt;color:#555;">\u00b7 {org_str}</span>' if org_str else ""
            yr_html = f' <span style="font-size:8pt;color:#777;">({yr_str})</span>' if yr_str else ""
            type_html = f' <span style="font-size:7.5pt;color:#888;">{atype}</span>' if atype else ""

            parts.append(
                '<div style="page-break-inside:avoid;border-bottom:1px solid #eee;'
                'padding:3px 0 3px 10px;">'
                f'<b style="font-size:9pt;">{esc(aw["award_name"])}</b>'
                + org_html + yr_html + type_html +
                "</div>"
            )
    return "\n".join(parts)


# ── Functional Mobility section ────────────────────────────────────────────────

def render_mobility(attributes: list[dict]) -> str:
    attr = {a["attribute_name"]: a for a in attributes}
    fsum = attr.get("functional_summary")
    fprof = attr.get("functional_profile")
    if not fsum and not fprof:
        return ""

    parts: list[str] = []

    if fsum:
        type_val = fsum.get("attribute_value") or ""
        type_color = MOBILITY_TYPE_COLORS.get(type_val, "#555")
        type_label = type_val.replace("_", " ").title()
        conf_val = fsum.get("confidence") or ""
        conf_html = (
            f' <span style="font-size:8pt;color:#666;">Confidence: {esc(conf_val)}</span>'
            if conf_val else ""
        )
        parts.append(
            '<div style="margin-bottom:5px;">'
            f'<span style="background:{type_color};color:#fff;padding:2px 8px;'
            f'border-radius:3px;font-size:9pt;font-weight:bold;">{esc(type_label)}</span>'
            + conf_html + "</div>"
        )
        extra = fsum.get("extra_data") or {}
        calib = extra.get("calibration_note") or ""
        if calib:
            parts.append(
                f'<p style="font-size:8.5pt;color:#444;margin:0 0 4px;">'
                f'<i>{esc(calib)}</i></p>'
            )

    if fprof:
        extra = fprof.get("extra_data") or {}
        arc = extra.get("functional_arc") or ""
        if arc:
            parts.append(
                f'<p style="font-size:8.5pt;margin:4px 0;">'
                f'<b>Functional Arc:</b> {esc(arc)}</p>'
            )
        primary = extra.get("primary_functions") or []
        if primary:
            parts.append(
                f'<p style="font-size:8.5pt;margin:4px 0;">'
                f'<b>Primary Functions:</b> {", ".join(esc(x) for x in primary)}</p>'
            )
        traj = extra.get("domain_trajectory") or []
        if traj:
            arrow = " \u2192 "
            parts.append(
                f'<p style="font-size:8.5pt;margin:4px 0;">'
                f'<b>Domain Trajectory:</b> {arrow.join(esc(x) for x in traj)}</p>'
            )
        transitions = extra.get("key_transitions") or []
        if transitions:
            items_html = ""
            for t in transitions:
                from_val = esc(t.get("from") or "")
                to_val   = esc(t.get("to") or "")
                yr       = t.get("approximate_year")
                yr_str   = f" (~{yr})" if yr else ""
                note     = esc(t.get("note") or "")
                note_str = f" \u2014 {note}" if note else ""
                items_html += f"<li>{from_val} \u2192 {to_val}{yr_str}{note_str}</li>"
            parts.append(
                '<p style="font-size:8.5pt;margin:4px 0;"><b>Key Transitions:</b></p>'
                f'<ul style="font-size:8.5pt;margin:0 0 4px 16px;">{items_html}</ul>'
            )

    return "\n".join(parts)


# ── Full page HTML ─────────────────────────────────────────────────────────────

def render_person_html(person: dict, positions: list[dict], education: list[dict],
                       awards: list[dict], attributes: list[dict],
                       notes: str | None) -> str:
    attr = {a["attribute_name"]: a for a in attributes}

    # Life-span
    if person.get("birth_year") and person.get("death_year"):
        lifespan = f"{person['birth_year']}\u2013{person['death_year']}"
    elif person.get("birth_year"):
        lifespan = f"b.\u00a0{person['birth_year']}"
    else:
        lifespan = ""

    deceased_html = ' <i style="color:#888;">deceased</i>' if person.get("death_status") == "deceased" else ""
    nats = esc(", ".join(person.get("nationalities") or []))
    hlp_str = f"HLP {esc(str(person.get('hlp_year', '')))} \u00b7 {esc(person.get('hlp_name', ''))}"
    nom_age = f"Nominated age\u00a0{person['hlp_nomination_age']}" if person.get("hlp_nomination_age") else ""

    meta_parts = [hlp_str]
    if lifespan:
        meta_parts.append(esc(lifespan) + deceased_html)
    if nats:
        meta_parts.append(nats)
    if nom_age:
        meta_parts.append(esc(nom_age))
    meta_line = " \u00b7 ".join(meta_parts)

    chips_html = "".join(
        outline_chip(label, attr[name]["attribute_value"], color)
        for name, label, color in CHIP_META
        if name in attr
    )
    chips_div = f'<div style="margin-bottom:8px;">{chips_html}</div>' if chips_html else ""

    notes_section = ""
    if notes:
        notes_section = section_wrap(
            "Notes",
            f'<p style="font-size:9pt;white-space:pre-wrap;">{esc(notes)}</p>',
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<style>
  @page {{ size: A4; margin: 20mm 18mm; }}
  body {{
    font-family: Helvetica, Arial, sans-serif;
    font-size: 9pt;
    color: #222;
    line-height: 1.4;
    margin: 0;
    padding: 0;
  }}
  h1 {{ font-size: 16pt; margin: 0 0 3px; }}
  table {{ border-collapse: collapse; width: 100%; }}
</style>
</head>
<body>
  <h1>{esc(person.get("display_name", ""))}</h1>
  <p style="font-size:8.5pt;color:#555;margin:0 0 5px;">{meta_line}</p>
  {chips_div}
  {section_wrap("Career Positions", render_career(positions))}
  {section_wrap("Education", render_education(education))}
  {section_wrap("Awards", render_awards(awards))}
  {section_wrap("Functional Mobility", render_mobility(attributes))}
  {notes_section}
</body>
</html>"""


# ── DB fetch ───────────────────────────────────────────────────────────────────

def fetch_person_data(conn, pid: int) -> tuple[dict, list, list, list, list, str | None]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

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
    """, {"pid": pid})
    person = dict(cur.fetchone())

    cur.execute("""
        SELECT cp.position_id, cp.title, cp.organization, cp.org_id,
               o.canonical_name AS org_canonical_name,
               cp.time_start, cp.time_finish, cp.approximate_period,
               cp.role_type, cp.confidence, cp.event_source, cp.sort_order,
               pt.domain, pt.organization_type, pt.un_placement, pt.geographic_scope,
               pt.role_type AS tag_role_type, pt.function, pt.career_phase, pt.policy_bridge
        FROM prosopography.career_positions cp
        LEFT JOIN prosopography.organizations o ON o.org_id = cp.org_id
        LEFT JOIN prosopography.position_tags pt ON pt.position_id = cp.position_id
        WHERE cp.person_id = %(pid)s
        ORDER BY cp.time_start NULLS LAST, cp.sort_order
    """, {"pid": pid})
    positions = [dict(r) for r in cur.fetchall()]

    pos_ids = [r["position_id"] for r in positions]
    ftag_map: dict[int, list[str]] = {}
    if pos_ids:
        cur.execute("""
            SELECT entity_id, tags FROM prosopography.user_functional_tags
            WHERE entity_type = 'position' AND entity_id = ANY(%(ids)s::integer[])
        """, {"ids": pos_ids})
        for row in cur.fetchall():
            ftag_map[row["entity_id"]] = row["tags"] or []
    for r in positions:
        r["functional_tags"] = ftag_map.get(r["position_id"], [])

    cur.execute("""
        SELECT education_id, degree_name, degree_type, field, institution,
               institution_country, time_start, time_finish, event_source, sort_order
        FROM prosopography.education
        WHERE person_id = %(pid)s
        ORDER BY time_start NULLS LAST, sort_order
    """, {"pid": pid})
    education = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT award_id, award_name, awarding_organization, award_type,
               time_start, confidence, event_source, sort_order
        FROM prosopography.awards
        WHERE person_id = %(pid)s
        ORDER BY time_start NULLS LAST, sort_order
    """, {"pid": pid})
    awards = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT attribute_name, attribute_value, attribute_label, confidence, extra_data
        FROM prosopography.person_attributes
        WHERE person_id = %(pid)s AND is_primary = true
        ORDER BY attribute_name
    """, {"pid": pid})
    attributes = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT note_text FROM prosopography.person_notes WHERE person_id = %(pid)s
    """, {"pid": pid})
    nrow = cur.fetchone()
    notes = nrow["note_text"] if nrow else None

    cur.close()
    return person, positions, education, awards, attributes, notes


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
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT person_id, display_name FROM prosopography.persons ORDER BY display_name"
        )
        persons = [dict(r) for r in cur.fetchall()]
        cur.close()

        total = len(persons)
        print(f"Generating PDFs for {total} persons -> {OUTPUT_DIR}")
        errors = 0
        for i, row in enumerate(persons, 1):
            pid = row["person_id"]
            fname = f"{pid:03d}_{slug(row['display_name'])}.pdf"
            out_path = OUTPUT_DIR / fname
            try:
                person, positions, education, awards, attributes, notes = fetch_person_data(conn, pid)
                html_str = render_person_html(person, positions, education, awards, attributes, notes)
                write_pdf(html_str, out_path)
                print(f"  [{i:02d}/{total}] {fname}")
            except Exception as exc:
                errors += 1
                print(f"  [{i:02d}/{total}] ERROR pid={pid}: {exc}", file=sys.stderr)
    finally:
        conn.close()

    if errors:
        print(f"\nCompleted with {errors} error(s).")
        sys.exit(1)
    print("\nDone.")


if __name__ == "__main__":
    main()

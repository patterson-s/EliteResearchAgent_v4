"""
Ontology annotation API.

Exposes endpoints for manually reviewing and mapping organizations into a
hierarchical, country-agnostic ontology. Supports multiple annotation runs
via CATEGORY_CONFIG — each category defines its own candidate detection query
and equivalence classes.
"""

import re
from typing import Optional
from fastapi import APIRouter, Query, HTTPException
from web.db import get_conn, rows_to_dicts, row_to_dict
from web.models import (
    OntologyRun,
    OntologyQueueItem,
    OntologyQueueResponse,
    OntologyProgress,
    OntologyMappingCreate,
    OntologyMappingResponse,
    OntologyEquivalenceClass,
    OntologyOrgPosition,
    OntologyOrgContext,
    OrgSplitRequest,
    OrgSplitResult,
    OrgSplitNewOrg,
    OntologyUserClass,
)

router = APIRouter()

# ── Hierarchy default parents ─────────────────────────────────────────────────
# Used to auto-compute hierarchy_path when parent_category is not explicitly set.

_DEFAULT_PARENT: dict[str, Optional[str]] = {
    # MFA
    "national_government":         None,
    "ministry_of_foreign_affairs": "national_government",
    "embassy":                     "ministry_of_foreign_affairs",
    "permanent_mission":           "ministry_of_foreign_affairs",
    "consulate":                   "ministry_of_foreign_affairs",
    "diplomatic_service":          "ministry_of_foreign_affairs",
    # Executive
    "executive_branch":            "national_government",
    "head_of_state":               "executive_branch",
    "head_of_government":          "executive_branch",
    "vice_head_of_state":          "executive_branch",
    "cabinet":                     "executive_branch",
    "executive_office":            "executive_branch",
    "national_security_council":   "executive_branch",
    "executive_advisory":          "executive_branch",
    "special_envoy":               "executive_branch",
    "presidential_campaign":       None,
    # IO (non-UN)
    "intergovernmental_organization": None,
    "multilateral_development_bank":  "intergovernmental_organization",
    "regional_organization":          "intergovernmental_organization",
    "military_security_alliance":     "intergovernmental_organization",
    "economic_cooperation_forum":     "intergovernmental_organization",
    "un_specialized_agency":          "intergovernmental_organization",
    "intergovernmental_panel":        "intergovernmental_organization",
    # Catch-all
    "not_mfa":                     None,
    "not_executive":               None,
    "not_io_body":                 None,
    "needs_review":                None,
}

_GRANDPARENT: dict[str, Optional[str]] = {
    # MFA level-3 sub-units
    "embassy":                   "national_government",
    "permanent_mission":         "national_government",
    "consulate":                 "national_government",
    "diplomatic_service":        "national_government",
    # Executive level-3 sub-units (parent=executive_branch, grandparent=national_government)
    "head_of_state":             "national_government",
    "head_of_government":        "national_government",
    "vice_head_of_state":        "national_government",
    "cabinet":                   "national_government",
    "executive_office":          "national_government",
    "national_security_council": "national_government",
    "executive_advisory":        "national_government",
    "special_envoy":             "national_government",
}


def _compute_hierarchy_path(
    equivalence_class: str,
    parent_category: Optional[str],
) -> list[str]:
    parent = parent_category or _DEFAULT_PARENT.get(equivalence_class)
    if not parent:
        return [equivalence_class] if equivalence_class not in ("not_mfa", "not_executive", "not_io_body", "needs_review") else []
    gp = _GRANDPARENT.get(equivalence_class)
    if gp:
        return [gp, parent, equivalence_class]
    return [parent, equivalence_class]


# ── Category configuration ────────────────────────────────────────────────────
# Each category defines:
#   equivalence_classes : ordered list of valid classes for this run
#   candidate_where     : SQL WHERE fragment identifying candidate orgs

CATEGORY_CONFIG: dict[str, dict] = {

    "mfa": {
        "equivalence_classes": [
            {"value": "national_government",         "label": "National Government",          "level": 1},
            {"value": "ministry_of_foreign_affairs", "label": "Ministry of Foreign Affairs",  "level": 2},
            {"value": "embassy",                     "label": "Embassy",                       "level": 3},
            {"value": "permanent_mission",           "label": "Permanent Mission (to IO)",     "level": 3},
            {"value": "consulate",                   "label": "Consulate",                     "level": 3},
            {"value": "diplomatic_service",          "label": "Diplomatic Service / Corps",    "level": 3},
            {"value": "not_mfa",                     "label": "Not MFA-related (exclude)",     "level": 0},
            {"value": "needs_review",                "label": "Needs Review",                  "level": 0},
        ],
        "candidate_where": """
            o.gov_canonical_tag ILIKE '%%foreignaffairs%%'
            OR 'national_government:ministries:foreign_affairs' = ANY(o.gov_hierarchical_tags)
            OR o.canonical_name ILIKE '%%ministry of foreign%%'
            OR o.canonical_name ILIKE '%%department of state%%'
            OR o.canonical_name ILIKE '%%foreign affairs%%'
            OR o.canonical_name ILIKE '%%foreign ministry%%'
            OR o.canonical_name ILIKE '%%embassy%%'
            OR o.canonical_name ILIKE '%%permanent mission%%'
            OR o.canonical_name ILIKE '%%diplomatic service%%'
        """,
    },

    "executive": {
        "equivalence_classes": [
            {"value": "national_government",       "label": "National Government",           "level": 1},
            {"value": "executive_branch",          "label": "Executive Branch",              "level": 2},
            {"value": "head_of_state",             "label": "Head of State",                 "level": 3},
            {"value": "head_of_government",        "label": "Head of Government",            "level": 3},
            {"value": "vice_head_of_state",        "label": "Vice / Deputy Head",            "level": 3},
            {"value": "cabinet",                   "label": "Cabinet / Council of Ministers", "level": 3},
            {"value": "executive_office",          "label": "Executive Office",              "level": 3},
            {"value": "national_security_council", "label": "National Security Council",     "level": 3},
            {"value": "executive_advisory",        "label": "Executive Advisory Body",       "level": 3},
            {"value": "special_envoy",             "label": "Envoy / Special Representative", "level": 3},
            {"value": "presidential_campaign",     "label": "Presidential Campaign",          "level": 0},
            {"value": "not_executive",             "label": "Not Executive (exclude)",        "level": 0},
            {"value": "needs_review",              "label": "Needs Review",                   "level": 0},
        ],
        "candidate_where": """
            EXISTS (SELECT 1 FROM prosopography.career_positions cp WHERE cp.org_id = o.org_id)
            AND (
                o.gov_canonical_tag IN ('executive', 'national_government:executive', 'national_government')
                OR 'national_government:executive' = ANY(o.gov_hierarchical_tags)
                OR o.canonical_name ILIKE '%%president%%'
                OR o.canonical_name ILIKE '%%prime minister%%'
                OR o.canonical_name ILIKE '%%premier%%'
                OR o.canonical_name ILIKE '%%chancellor%%'
                OR o.canonical_name ILIKE '%%cabinet%%'
                OR o.canonical_name ILIKE '%%council of ministers%%'
                OR o.canonical_name ILIKE '%%executive office%%'
                OR o.canonical_name ILIKE '%%white house%%'
                OR o.canonical_name ILIKE '%%kremlin%%'
                OR o.canonical_name ILIKE '%%federal council%%'
                OR o.canonical_name ILIKE '%%office of the president%%'
                OR o.canonical_name ILIKE '%%office of the prime%%'
            )
        """,
    },

    "io_non_un": {
        "equivalence_classes": [
            {"value": "intergovernmental_organization", "label": "Intergovernmental Organization",       "level": 1},
            {"value": "multilateral_development_bank",  "label": "Multilateral Development Bank",        "level": 2},
            {"value": "regional_organization",          "label": "Regional Organization",                "level": 2},
            {"value": "military_security_alliance",     "label": "Military / Security Alliance",         "level": 2},
            {"value": "economic_cooperation_forum",     "label": "Economic Cooperation Forum",           "level": 2},
            {"value": "un_specialized_agency",          "label": "UN Specialized Agency (autonomous)",   "level": 2},
            {"value": "intergovernmental_panel",        "label": "Intergovernmental Panel / Commission", "level": 2},
            {"value": "not_io_body",                    "label": "Not IO Body (exclude)",                "level": 0},
            {"value": "needs_review",                   "label": "Needs Review",                         "level": 0},
        ],
        "candidate_where": """
            o.meta_type = 'io'
            AND o.canonical_name NOT ILIKE '%%United Nations%%'
            AND o.canonical_name NOT ILIKE '%%Nations Unies%%'
            AND o.canonical_name NOT ILIKE 'UN %%'
            AND o.canonical_name NOT ILIKE '%%UNAIDS%%'
            AND o.canonical_name NOT ILIKE '%%UNDP%%'
        """,
    },
}


def _get_category(category: str) -> dict:
    cfg = CATEGORY_CONFIG.get(category)
    if not cfg:
        raise HTTPException(status_code=400, detail=f"Unknown category '{category}'. Valid: {list(CATEGORY_CONFIG)}")
    return cfg


# ── Runs ──────────────────────────────────────────────────────────────────────

@router.get("/runs", response_model=list[OntologyRun])
def list_ontology_runs():
    """List all derivative runs with entity_level='organization'."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT run_id, run_name, narrative, evaluation_status, n_processed,
                   scope_json->>'category' AS category
            FROM prosopography.derivative_runs
            WHERE entity_level = 'organization'
            ORDER BY run_id
        """)
        rows = rows_to_dicts(cur)
        cur.close()
    return [OntologyRun(**r) for r in rows]


# ── Queue ─────────────────────────────────────────────────────────────────────

@router.get("/queue/{category}", response_model=OntologyQueueResponse)
def get_queue(
    category: str,
    run_id: int = Query(...),
    limit: int = Query(200, le=500),
    offset: int = Query(0),
):
    cfg = _get_category(category)
    where = cfg["candidate_where"]

    queue_sql = f"""
        SELECT
            o.org_id, o.canonical_name, o.meta_type, o.gov_canonical_tag,
            o.gov_hierarchical_tags, o.location_country,
            (oom.mapping_id IS NOT NULL) AS is_reviewed,
            oom.mapping_id, oom.equivalence_class, oom.country_code,
            oom.destination_country, oom.destination_organization, oom.superior,
            oom.parent_category, oom.hierarchy_path, oom.display_label,
            oom.annotation_notes, oom.region
        FROM prosopography.organizations o
        LEFT JOIN prosopography.org_ontology_mappings oom
            ON oom.org_id = o.org_id AND oom.run_id = %(run_id)s
        WHERE {where}
        ORDER BY (oom.mapping_id IS NOT NULL) ASC, o.location_country NULLS LAST, o.canonical_name
        LIMIT %(limit)s OFFSET %(offset)s
    """
    count_sql = f"""
        SELECT COUNT(DISTINCT o.org_id)
        FROM prosopography.organizations o
        WHERE {where}
    """
    reviewed_sql = f"""
        SELECT COUNT(*)
        FROM prosopography.org_ontology_mappings oom
        JOIN prosopography.organizations o ON o.org_id = oom.org_id
        WHERE oom.run_id = %(run_id)s AND ({where})
    """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(count_sql)
        total = cur.fetchone()[0]
        cur.execute(reviewed_sql, {"run_id": run_id})
        reviewed = cur.fetchone()[0]
        cur.execute(queue_sql, {"run_id": run_id, "limit": limit, "offset": offset})
        rows = rows_to_dicts(cur)
        cur.close()

    items = [OntologyQueueItem(**r) for r in rows]
    return OntologyQueueResponse(
        run_id=run_id, category=category,
        total=total, reviewed=reviewed, remaining=total - reviewed,
        items=items,
    )


# ── Progress ──────────────────────────────────────────────────────────────────

@router.get("/progress/{category}", response_model=OntologyProgress)
def get_progress(category: str, run_id: int = Query(...)):
    cfg = _get_category(category)
    where = cfg["candidate_where"]

    count_sql = f"SELECT COUNT(DISTINCT o.org_id) FROM prosopography.organizations o WHERE {where}"
    reviewed_sql = f"""
        SELECT COUNT(*) FROM prosopography.org_ontology_mappings oom
        JOIN prosopography.organizations o ON o.org_id = oom.org_id
        WHERE oom.run_id = %(run_id)s AND ({where})
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(count_sql)
        total = cur.fetchone()[0]
        cur.execute(reviewed_sql, {"run_id": run_id})
        reviewed = cur.fetchone()[0]
        cur.close()
    return OntologyProgress(run_id=run_id, category=category, total=total, reviewed=reviewed, remaining=total - reviewed)


# ── Autocomplete ──────────────────────────────────────────────────────────────

@router.get("/autocomplete/equivalence-classes", response_model=list[OntologyEquivalenceClass])
def get_equivalence_classes(category: Optional[str] = Query(None)):
    """Return hardcoded equivalence classes merged with user-defined classes from DB."""
    # Build hardcoded list with parent_class populated from _DEFAULT_PARENT
    if category:
        cfg = CATEGORY_CONFIG.get(category)
        hardcoded = list(cfg["equivalence_classes"]) if cfg else []
    else:
        seen: set[str] = set()
        hardcoded = []
        for cfg in CATEGORY_CONFIG.values():
            for c in cfg["equivalence_classes"]:
                if c["value"] not in seen:
                    seen.add(c["value"])
                    hardcoded.append(c)

    # Attach parent_class to each hardcoded entry
    result = [
        OntologyEquivalenceClass(
            value=c["value"],
            label=c["label"],
            level=c["level"],
            parent_class=_DEFAULT_PARENT.get(c["value"]),
        )
        for c in hardcoded
    ]

    # Merge user-defined classes from DB
    hardcoded_levels = {c["value"]: c["level"] for cfg in CATEGORY_CONFIG.values() for c in cfg["equivalence_classes"]}
    with get_conn() as conn:
        cur = conn.cursor()
        if category:
            cur.execute("""
                SELECT value, label, parent_class FROM prosopography.ontology_user_classes
                WHERE category = %(cat)s ORDER BY parent_class, label
            """, {"cat": category})
        else:
            cur.execute("""
                SELECT value, label, parent_class FROM prosopography.ontology_user_classes
                ORDER BY category, parent_class, label
            """)
        for row in cur.fetchall():
            value, label, parent_class = row
            parent_level = hardcoded_levels.get(parent_class, 3)
            result.append(OntologyEquivalenceClass(
                value=value,
                label=label,
                level=parent_level + 1,
                parent_class=parent_class,
            ))
        cur.close()

    return result


@router.get("/autocomplete/countries", response_model=list[str])
def get_countries():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT location_country FROM prosopography.organizations
            WHERE location_country IS NOT NULL AND location_country != 'unknown'
            ORDER BY location_country
        """)
        countries = [r[0] for r in cur.fetchall()]
        cur.close()
    return countries


# ── Mappings (write) ──────────────────────────────────────────────────────────

@router.post("/mappings", response_model=OntologyMappingResponse)
def save_mapping(body: OntologyMappingCreate):
    hierarchy_path = body.hierarchy_path or _compute_hierarchy_path(body.equivalence_class, body.parent_category)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO prosopography.org_ontology_mappings (
                org_id, run_id, equivalence_class, country_code, destination_country,
                destination_organization, superior, parent_category, hierarchy_path,
                display_label, annotation_notes, region, annotated_by, updated_at
            )
            VALUES (
                %(org_id)s, %(run_id)s, %(equivalence_class)s, %(country_code)s, %(destination_country)s,
                %(destination_organization)s, %(superior)s, %(parent_category)s, %(hierarchy_path)s,
                %(display_label)s, %(annotation_notes)s, %(region)s, 'manual', now()
            )
            ON CONFLICT (org_id, run_id) DO UPDATE SET
                equivalence_class        = EXCLUDED.equivalence_class,
                country_code             = EXCLUDED.country_code,
                destination_country      = EXCLUDED.destination_country,
                destination_organization = EXCLUDED.destination_organization,
                superior                 = EXCLUDED.superior,
                parent_category          = EXCLUDED.parent_category,
                hierarchy_path           = EXCLUDED.hierarchy_path,
                display_label            = EXCLUDED.display_label,
                annotation_notes         = EXCLUDED.annotation_notes,
                region                   = EXCLUDED.region,
                updated_at               = now()
            RETURNING
                mapping_id, org_id, run_id, equivalence_class, country_code, destination_country,
                destination_organization, superior, parent_category, hierarchy_path,
                display_label, annotation_notes, region, annotated_by
        """, {
            "org_id":                    body.org_id,
            "run_id":                    body.run_id,
            "equivalence_class":         body.equivalence_class,
            "country_code":              body.country_code,
            "destination_country":       body.destination_country,
            "destination_organization":  body.destination_organization,
            "superior":                  body.superior,
            "parent_category":           body.parent_category,
            "hierarchy_path":            hierarchy_path,
            "display_label":             body.display_label,
            "annotation_notes":          body.annotation_notes,
            "region":                    body.region,
        })
        row = row_to_dict(cur, cur.fetchone())

        # Auto-upsert user-defined class if this is a new sub-class
        _hardcoded = {c["value"] for cfg in CATEGORY_CONFIG.values() for c in cfg["equivalence_classes"]}
        if body.new_class_label and body.equivalence_class not in _hardcoded:
            # Derive category from the run
            cur.execute("""
                SELECT scope_json->>'category' FROM prosopography.derivative_runs WHERE run_id = %(run_id)s
            """, {"run_id": body.run_id})
            cat_row = cur.fetchone()
            category = cat_row[0] if cat_row else "unknown"
            cur.execute("""
                INSERT INTO prosopography.ontology_user_classes (value, label, parent_class, category)
                VALUES (%(value)s, %(label)s, %(parent)s, %(cat)s)
                ON CONFLICT (value) DO NOTHING
            """, {
                "value":  body.equivalence_class,
                "label":  body.new_class_label,
                "parent": body.parent_category or "",
                "cat":    category,
            })

        conn.commit()
        cur.close()
    return OntologyMappingResponse(**row)


@router.get("/orgs/{org_id}/context", response_model=OntologyOrgContext)
def get_org_context(org_id: int, direct_only: bool = Query(False)):
    """Return career positions linked to this org, with person and HLP context.

    Three-tier cascade:
      1. Direct org_id FK match (most reliable)
      2. Phrase match on cp.organization text (handles duplicate org entries)
      3. Sibling org lookup — other orgs in same country with similar name
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # Tier 1: direct org_id FK — one row per unique (person, title)
        cur.execute("""
            SELECT p.person_id, p.display_name, h.hlp_name,
                   cp.title, MIN(cp.time_start) AS time_start, MAX(cp.time_finish) AS time_finish
            FROM prosopography.career_positions cp
            JOIN prosopography.persons p ON p.person_id = cp.person_id
            JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
            WHERE cp.org_id = %(org_id)s
            GROUP BY p.person_id, p.display_name, h.hlp_name, cp.title
            ORDER BY p.display_name, MIN(cp.time_start) NULLS LAST
        """, {"org_id": org_id})
        rows = rows_to_dicts(cur)
        if rows:
            cur.close()
            return OntologyOrgContext(org_id=org_id, match_type="direct",
                                      positions=[OntologyOrgPosition(**r) for r in rows])

        if direct_only:
            cur.close()
            return OntologyOrgContext(org_id=org_id, match_type="none", positions=[])

        # Get org metadata for fallback tiers
        cur.execute("""
            SELECT canonical_name, location_country
            FROM prosopography.organizations WHERE org_id = %(org_id)s
        """, {"org_id": org_id})
        org_row = cur.fetchone()
        if not org_row:
            cur.close()
            return OntologyOrgContext(org_id=org_id, match_type="none", positions=[])

        canonical_name, country = org_row

        # Tier 2: phrase match on cp.organization (handles duplicate org_ids for same institution).
        # Strip parenthetical alternatives first: "Swiss Federal Council (Federal Council of
        # Switzerland)" → "Swiss Federal Council", so the phrase matches extracted career text.
        phrase_name = re.sub(r'\s*\(.*?\)', '', canonical_name).strip()
        if len(phrase_name) >= 6:
            cur.execute("""
                SELECT p.person_id, p.display_name, h.hlp_name,
                       cp.title, MIN(cp.time_start) AS time_start, MAX(cp.time_finish) AS time_finish
                FROM prosopography.career_positions cp
                JOIN prosopography.persons p ON p.person_id = cp.person_id
                JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
                WHERE cp.organization ILIKE %(phrase)s
                GROUP BY p.person_id, p.display_name, h.hlp_name, cp.title
                ORDER BY p.display_name, MIN(cp.time_start) NULLS LAST
                LIMIT 20
            """, {"phrase": f"%{phrase_name}%"})
            rows = rows_to_dicts(cur)
            if rows:
                cur.close()
                return OntologyOrgContext(org_id=org_id, match_type="approximate",
                                          positions=[OntologyOrgPosition(**r) for r in rows])

        # Tier 3: sibling org lookup — other orgs in same country sharing distinctive keywords
        if country:
            stopwords = {
                'the', 'of', 'and', 'for', 'in', 'on', 'at', 'to', 'a', 'an',
                'by', 'from', 'with', 'de', 'du', 'des', 'la', 'le', 'les',
            }
            clean = (canonical_name.replace('/', ' ').replace('-', ' ')
                     .replace('(', '').replace(')', ''))
            keywords = [w for w in clean.split() if len(w) >= 6 and w.lower() not in stopwords]
            if keywords:
                org_conds = " OR ".join(
                    f"o2.canonical_name ILIKE %(kw_{i})s" for i in range(len(keywords))
                )
                params: dict = {f"kw_{i}": f"%{kw}%" for i, kw in enumerate(keywords)}
                params.update({"org_id": org_id, "country": country})
                cur.execute(f"""
                    SELECT p.person_id, p.display_name, h.hlp_name,
                           cp.title, MIN(cp.time_start) AS time_start, MAX(cp.time_finish) AS time_finish
                    FROM prosopography.organizations o2
                    JOIN prosopography.career_positions cp ON cp.org_id = o2.org_id
                    JOIN prosopography.persons p ON p.person_id = cp.person_id
                    JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
                    WHERE o2.org_id != %(org_id)s
                      AND o2.location_country = %(country)s
                      AND ({org_conds})
                    GROUP BY p.person_id, p.display_name, h.hlp_name, cp.title
                    ORDER BY p.display_name, MIN(cp.time_start) NULLS LAST
                    LIMIT 20
                """, params)
                rows = rows_to_dicts(cur)
                if rows:
                    cur.close()
                    return OntologyOrgContext(org_id=org_id, match_type="sibling",
                                              positions=[OntologyOrgPosition(**r) for r in rows])

        cur.close()
    return OntologyOrgContext(org_id=org_id, match_type="none", positions=[])


@router.post("/orgs/{org_id}/split", response_model=OrgSplitResult)
def split_org(org_id: int, req: OrgSplitRequest):
    """Split one org into multiple by reassigning career positions by title.

    Creates a new organizations row for each split spec, copying metadata from
    the parent. Reassigns career_positions.org_id for each title group to its
    new org. The original org retains any positions not covered by the split specs.
    """
    if not req.splits:
        raise HTTPException(status_code=400, detail="At least one split spec required.")

    with get_conn() as conn:
        cur = conn.cursor()

        # Load parent org metadata
        cur.execute("""
            SELECT canonical_name, meta_type, org_types, sector,
                   location_country, location_city,
                   gov_canonical_tag, gov_hierarchical_tags, gov_country
            FROM prosopography.organizations
            WHERE org_id = %(org_id)s
        """, {"org_id": org_id})
        row = cur.fetchone()
        if not row:
            cur.close()
            raise HTTPException(status_code=404, detail=f"Org {org_id} not found.")
        (parent_name, meta_type, org_types, sector,
         location_country, location_city,
         gov_canonical_tag, gov_hierarchical_tags, gov_country) = row

        new_orgs = []
        for spec in req.splits:
            # Create new org, inheriting parent metadata
            cur.execute("""
                INSERT INTO prosopography.organizations
                    (canonical_name, meta_type, org_types, sector,
                     location_country, location_city,
                     gov_canonical_tag, gov_hierarchical_tags, gov_country,
                     source, review_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING org_id
            """, [
                spec.new_canonical_name, meta_type, org_types, sector,
                location_country, location_city,
                gov_canonical_tag, gov_hierarchical_tags, gov_country,
                f"split_from_{org_id}", "pending_review",
            ])
            new_org_id = cur.fetchone()[0]

            # Reassign matching career positions
            cur.execute("""
                UPDATE prosopography.career_positions
                SET org_id = %s
                WHERE org_id = %s AND title = ANY(%s)
            """, [new_org_id, org_id, spec.titles])
            count = cur.rowcount

            new_orgs.append(OrgSplitNewOrg(
                org_id=new_org_id,
                canonical_name=spec.new_canonical_name,
                position_count=count,
            ))

        conn.commit()
        cur.close()

    return OrgSplitResult(original_org_id=org_id, new_orgs=new_orgs)


@router.delete("/mappings/{mapping_id}", status_code=204)
def delete_mapping(mapping_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM prosopography.org_ontology_mappings WHERE mapping_id = %s RETURNING mapping_id",
            (mapping_id,),
        )
        deleted = cur.fetchone()
        conn.commit()
        cur.close()
    if not deleted:
        raise HTTPException(status_code=404, detail="Mapping not found")

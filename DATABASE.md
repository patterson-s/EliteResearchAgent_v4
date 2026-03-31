# Prosopography Database — Reference Guide

This document describes the contents, structure, and usage of the `prosopography` PostgreSQL schema. It is intended as a handoff reference for collaborators.

---

## Project Overview

This is a **prosopography database** for the 75 members of four UN Secretary-General High-Level Panels (HLPs). The goal is to support research on elite career trajectories, organizational networks, and typological analysis of global governance actors.

The database was built in two stages:
1. **Base data** — biographical records, career histories, education, awards, and organizational metadata migrated from v3 pipeline outputs
2. **Derivatives** — LLM-generated analytical tags and classifications stored alongside base data with provenance tracking

A web interface for browsing the data is available (see [Running the Web Interface](#running-the-web-interface)).

---

## The Four HLP Panels

| ID | Panel Name | Year | UN SG |
|----|-----------|------|-------|
| 1 | Threats, Challenges and Change | 2004 | Kofi Annan |
| 2 | System-Wide Coherence | 2007 | Kofi Annan |
| 3 | Post-2015 Development Agenda | 2012 | Ban Ki-moon |
| 4 | Digital Cooperation | 2020 | António Guterres |

Each panel has 15–25 members (75 total across all four).

---

## Database Connection

**Database:** `eliteresearch` (PostgreSQL)
**Schema:** `prosopography`
**Host:** localhost (Jetson Orin Nano or local dev machine)

Connection via Python (credentials in `.env` in project root):

```python
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", 5432)),
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)
```

All tables live in the `prosopography` schema — always prefix table names: `prosopography.persons`, `prosopography.career_positions`, etc.

---

## Schema Overview

### Row Counts (as of last migration)

| Table | Rows | Description |
|-------|------|-------------|
| `hlp_panels` | 4 | HLP reference — one row per panel |
| `persons` | 75 | One row per elite |
| `person_nationalities` | 79 | Multi-valued; some persons have dual nationality |
| `career_positions` | 2,183 | Career history entries |
| `education` | 219 | Educational credentials |
| `awards` | 715 | Prizes, honorary degrees, fellowships |
| `pipeline_runs` | 75 | Metadata for the v3 extraction pipeline (one per person) |
| `biographical_provenance` | 301 | Audit trail for birth year, nationality, death status |
| `organizations` | 2,619 | Canonical organization entries |
| `organization_aliases` | 209 | Alternative names and abbreviations |
| `derivative_runs` | 4 | Provenance records for analytical runs |
| `position_tags` | 2,181 | Per-position analytical tags |
| `person_attributes` | 414 | Person-level analytical attributes |

---

## Core Tables

### `persons`

Central table — one row per HLP member.

| Column | Type | Notes |
|--------|------|-------|
| `person_id` | SERIAL PK | Internal identifier |
| `person_dir_name` | TEXT UNIQUE | Filesystem key, e.g. `Abhijit_Banerjee` |
| `display_name` | TEXT | Human-readable name |
| `sources_person_name` | TEXT | Name as it appeared in source data |
| `birth_year` | SMALLINT | |
| `death_status` | TEXT | `alive`, `deceased`, or `unknown` |
| `death_year` | SMALLINT | NULL if alive |
| `hlp_id` | INTEGER FK | Which panel they served on |
| `hlp_nomination_age` | SMALLINT | Age at time of panel |
| `data_status` | TEXT | `base` (all current rows) |

**Linked tables:** `person_nationalities` (multi-valued nationality), `career_positions`, `education`, `awards`, `biographical_provenance`, `person_attributes`

---

### `career_positions`

~2,183 rows. The richest table — each row is one career event.

| Column | Type | Notes |
|--------|------|-------|
| `position_id` | SERIAL PK | |
| `person_id` | INTEGER FK | |
| `title` | TEXT | Job title |
| `organization` | TEXT | Original organization string from source |
| `org_id` | INTEGER FK | Canonical org match (NULL for ~46 unmatched) |
| `org_match_method` | TEXT | `exact`, `alias`, or `fuzzy` |
| `time_start` | SMALLINT | Year |
| `time_finish` | SMALLINT | Year (NULL = ongoing at time of extraction) |
| `approximate_period` | TEXT | Human-readable period string, e.g. `2010–2015` |
| `role_type` | TEXT | `primary`, `advisory`, `governance`, `other` |
| `confidence` | TEXT | `high`, `medium`, `low` |
| `event_source` | TEXT | `wikipedia` or `gap_finding` |
| `verified_sources` | JSONB | URLs and titles of confirming sources |
| `supporting_quotes` | JSONB | Extracted text evidence |
| `data_status` | TEXT | `base` |

**Organization matching:** 2,027 exact matches, 110 fuzzy, 46 alias (93.8% of positions have a canonical `org_id`).

---

### `education`

219 rows. Educational credentials per person.

Key columns: `degree_name`, `degree_type` (`undergraduate`, `masters`, `doctoral`, `postdoctoral`, `professional`, `certificate`, `other`), `field`, `institution`, `institution_country`, `time_start`, `time_finish`.

---

### `awards`

715 rows. Prizes, honorary degrees, fellowships, medals.

Key columns: `award_name`, `awarding_organization`, `award_type` (`recognition`, `prize`, `honorary_degree`, `medal`, `fellowship`, `other`), `time_start`, `confidence`.

---

### `organizations`

2,619 rows. Canonical organization entries, drawn from the v3 ontology (1,609) plus auto-created stubs for unmatched career organizations (1,010 stubs with `review_status = 'pending_review'`).

| Column | Type | Notes |
|--------|------|-------|
| `org_id` | SERIAL PK | |
| `canonical_name` | TEXT UNIQUE | |
| `meta_type` | TEXT | `io`, `gov`, `university`, `ngo`, `private`, `other` |
| `sector` | TEXT | e.g. `intergovernmental`, `academic`, `civil_society` |
| `location_country` | TEXT | ISO country code |
| `location_city` | TEXT | |
| `un_canonical_tag` | TEXT | UN sub-ontology tag (85 UN-affiliated orgs) |
| `un_hierarchical_tags` | TEXT[] | e.g. `{United Nations, UN, UN:FundsAndProgrammes, UN:FundsAndProgrammes:UNICEF}` |
| `gov_canonical_tag` | TEXT | Government sub-ontology tag |
| `gov_hierarchical_tags` | TEXT[] | e.g. `{United Kingdom, UK:ExecutiveBranch, UK:ExecutiveBranch:Cabinet}` |
| `review_status` | TEXT | `base`, `completed`, or `pending_review` (stubs) |

**Meta-type breakdown:** io: 85, gov: 386, university: 229, ngo: 115, private: 39, other: 1,765 (includes stubs)

**Top organizations by corpus members:** United Nations (51), World Bank (12), World Economic Forum (12), African Union (7), Club de Madrid (7), African Development Bank (6)

---

### `biographical_provenance`

301 rows. Audit trail for the LLM pipeline that extracted biographical facts. One row per person per field per pipeline run.

Fields tracked: `birth_year`, `death_status`, `death_year`, `nationality`

Each row stores the full pipeline trace: `retrieval_json`, `extractions_json`, `verification_json`, `substantiation_json` (all JSONB), plus `model_used`, `verified_answer`, `verification_status`, and `referenced_chunk_ids` (INTEGER[]).

---

## Derivative Tables

Derivatives are LLM-generated analytical outputs, stored separately from base data with explicit provenance.

### `derivative_runs`

4 rows. One row per analytical pipeline run — the provenance header for all derivative data.

| run_id | run_name | type | level | n_processed | status |
|--------|----------|------|-------|-------------|--------|
| 1 | career_tags_v1 | position_tagging | position | 2,183 | draft |
| 2 | career_domain_v1 | career_domain | person | 75 | draft |
| 3 | geo_profile_v1 | geo_trajectory | person | 69 | draft |
| 4 | ideal_types_v1 | person_typology | person | 55 | draft |

All current runs are `evaluation_status = 'draft'`. Update to `reviewed` or `validated` after spot-checking.

---

### `position_tags`

2,181 rows. One row per career position — analytical tags across 8 dimensions.

| Column | Values (examples) |
|--------|------------------|
| `domain` | `TEXT[]` — `[education_research, economic_policy_finance]` |
| `organization_type` | `university_research`, `io_un`, `io_non_un`, `gov_executive`, `foundation_philanthropy`, `ngo`, `private_sector`, `independent_advisory` |
| `un_placement` | `none`, `affiliated_org`, `core_un` |
| `geographic_scope` | `national`, `regional`, `global`, `subnational` |
| `role_type` | `academic_appointment`, `executive_head`, `board_governance`, `staff_officer`, `professional_practitioner`, `founder` |
| `function` | `academic_research`, `policy_advisory`, `executive_management`, `governance_oversight`, `entrepreneurial`, `technical_expertise` |
| `career_phase` | `formative`, `consolidation`, `apex`, `post_apex`, `unknown` |
| `policy_bridge` | `BOOLEAN` |

**Phase distribution:** unknown: 693, formative: 481, consolidation: 426, post_apex: 357, apex: 224

---

### `person_attributes`

414 rows. Person-level analytical attributes — one row per person per attribute.

| attribute_name | Coverage | Values |
|----------------|----------|--------|
| `career_domain` | 75/75 | political (28), international (20), diplomatic (12), civil_society (5), academic (5), corporate (5) |
| `is_hybrid_domain` | 75/75 | true/false |
| `career_typology` | 55/75 | see below |
| `mobility_pattern` | 73/75 | moderately_mobile (47), nationally_grounded (15), highly_mobile (11) |
| `geo_edu_category` | 68/75 | global_north (40), both (16), global_south (12) |
| `institution_prestige` | 68/75 | elite (24), both (24), peripheral (19) |

**Career typology (ideal types):** 7 types defined from qualitative framework:

| Code | Label | Count |
|------|-------|-------|
| `DOMESTIC_POLITICAL_ELDER` | National Political Leader as Global Elder | 11 |
| `NATIONAL_TO_GLOBAL_PIVOT` | National to Global Pivot | 10 |
| `DEVELOPMENT_CIRCUIT_RIDER` | Development Circuit Rider | 9 |
| `CAREER_FOREIGN_SERVICE` | Career Foreign Service | 9 |
| `DOMAIN_KNOWLEDGE_AUTHORITY` | Domain Knowledge Authority | 8 |
| `CIVIL_SOCIETY_PLATFORM_BUILDER` | Civil Society Platform Builder | 5 |
| `CORPORATE_TO_GOVERNANCE_CROSSOVER` | Corporate to Governance Crossover | 3 |

Full type definitions (descriptions, key features, dimension signatures, exemplars) are in:
`eliteresearchagent_v3/analysis/ideal_types/outputs/ideal_type_definitions.json`

---

## Useful Queries

### All persons in a panel with their career domain
```sql
SELECT p.display_name, pa.attribute_value AS career_domain,
       pt2.attribute_value AS career_typology
FROM prosopography.persons p
JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
LEFT JOIN prosopography.person_attributes pa
    ON pa.person_id = p.person_id AND pa.attribute_name = 'career_domain'
LEFT JOIN prosopography.person_attributes pt2
    ON pt2.person_id = p.person_id AND pt2.attribute_name = 'career_typology'
WHERE h.hlp_year = 2012
ORDER BY p.display_name;
```

### Career positions with organization metadata for a person
```sql
SELECT cp.title, cp.organization, o.meta_type, o.sector,
       o.un_hierarchical_tags, cp.time_start, cp.time_finish,
       cp.role_type, pt.career_phase, pt.domain
FROM prosopography.career_positions cp
LEFT JOIN prosopography.organizations o ON o.org_id = cp.org_id
LEFT JOIN prosopography.position_tags pt ON pt.position_id = cp.position_id
WHERE cp.person_id = 1  -- replace with target person_id
ORDER BY cp.time_start NULLS LAST;
```

### All persons assigned a given ideal type
```sql
SELECT p.display_name, h.hlp_name, pa.attribute_value AS typology,
       pa2.attribute_value AS career_domain
FROM prosopography.person_attributes pa
JOIN prosopography.persons p ON p.person_id = pa.person_id
JOIN prosopography.hlp_panels h ON h.hlp_id = p.hlp_id
LEFT JOIN prosopography.person_attributes pa2
    ON pa2.person_id = p.person_id AND pa2.attribute_name = 'career_domain'
WHERE pa.attribute_name = 'career_typology'
  AND pa.attribute_value = 'DOMESTIC_POLITICAL_ELDER'
ORDER BY h.hlp_year, p.display_name;
```

### Organization co-membership (persons who overlapped at the same org)
```sql
SELECT o.canonical_name, o.meta_type,
       array_agg(DISTINCT p.display_name ORDER BY p.display_name) AS members,
       COUNT(DISTINCT p.person_id) AS n_members
FROM prosopography.career_positions cp
JOIN prosopography.organizations o ON o.org_id = cp.org_id
JOIN prosopography.persons p ON p.person_id = cp.person_id
WHERE o.review_status != 'pending_review'
GROUP BY o.org_id, o.canonical_name, o.meta_type
HAVING COUNT(DISTINCT p.person_id) >= 3
ORDER BY n_members DESC;
```

### Career phase distribution by ideal type
```sql
SELECT pa.attribute_value AS typology, pt.career_phase, COUNT(*) AS n_positions
FROM prosopography.position_tags pt
JOIN prosopography.career_positions cp ON cp.position_id = pt.position_id
JOIN prosopography.person_attributes pa
    ON pa.person_id = cp.person_id AND pa.attribute_name = 'career_typology'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

## Running the Web Interface

The project includes a read-only web browser for exploring persons, their career trajectories, and organizations.

**Requirements:** `eliteresearchagent_v3` conda environment (Python 3.12, FastAPI, uvicorn, psycopg2 installed)

**Start the server:**
```
serve.bat
```
Then open **http://localhost:8000** in your browser.

**Features:**
- **Persons view** — sidebar list of all 75 members; filter by HLP panel, career domain, career type, nationality, birth decade
- **Person profile** — bio header with analytical attribute chips (domain, typology, mobility, prestige), career timeline with career phase + domain badges on each position, education tab, awards tab
- **Organization hover tooltips** — hover any org name in a career timeline to see type, sector, UN hierarchy, and corpus member count
- **Organizations view** — browse all 2,619 orgs; filter by type and sector; click for full profile with all corpus members
- **Global search** — top-right search bar finds persons and organizations

---

## File Structure

```
eliteresearchagent_v4/
├── .env                        # DB credentials (not in version control)
├── requirements.txt            # Python dependencies
├── serve.bat                   # Start the web server
├── instructions.md             # Original project brief
├── DATABASE.md                 # This document
│
├── db/                         # Migration scripts (run once, in order)
│   ├── db_utils.py             # Shared DB connection
│   ├── migrate_01_create_schema.py          # Core schema DDL
│   ├── migrate_02_load_persons.py           # Load 75 persons
│   ├── migrate_03_load_career_events.py     # Load positions/education/awards
│   ├── migrate_04_load_biographical_provenance.py
│   ├── migrate_05_validate.py              # Integrity checks
│   ├── migrate_06_create_org_schema.py     # Organizations schema
│   ├── migrate_07_load_organizations.py    # Load org ontology
│   ├── migrate_08_match_positions.py       # Link positions → orgs
│   ├── migrate_09_create_derivatives_schema.py  # Derivative tables
│   ├── migrate_10_load_career_tags.py      # Load position tags
│   └── migrate_11_load_person_attributes.py    # Load person attributes
│
└── web/                        # Web interface (FastAPI + vanilla JS)
    ├── app.py                  # FastAPI app + router registration
    ├── db.py                   # DB connection helper
    ├── models.py               # Pydantic response models
    ├── routers/
    │   ├── persons.py          # /api/persons endpoints
    │   ├── organizations.py    # /api/organizations endpoints
    │   ├── search.py           # /api/search endpoint
    │   └── hlp.py              # /api/hlp endpoint
    └── static/
        └── index.html          # Single-page application (~1,000 lines)
```

---

## Data Provenance & Caveats

- **Base data source:** All base records were extracted from web sources (Wikipedia, gap-finding searches) by an LLM pipeline in `eliteresearchagent_v3`. Source URLs and supporting quotes are stored in `verified_sources` and `supporting_quotes` JSONB columns.
- **Derivative status:** All four derivative runs are currently `evaluation_status = 'draft'`. They have not been systematically validated. Use with appropriate caution for analysis.
- **Coverage gaps:**
  - `career_typology` is missing for 20/75 persons (not in original v3 `person_map.json`)
  - `geo_edu_category` and `institution_prestige` are missing for 7/75 persons
  - `mobility_pattern` is missing for 2/75 persons
- **Organization stubs:** 1,010 of 2,619 organizations are auto-created stubs (`review_status = 'pending_review'`) for organization names that appeared in career histories but weren't in the v3 ontology. These have `meta_type = 'other'` and minimal metadata.
- **Career phase `unknown`:** 693 of 2,181 tagged positions (31.8%) received `career_phase = 'unknown'` — typically advisory or governance roles that don't fit cleanly into a career arc.

---

## Adding New Derivatives

To add a new analytical run:

1. **Insert a `derivative_runs` row** with full provenance metadata (model, prompt, config, narrative, replication notes)
2. **Insert derivative data** into `position_tags` (position-level) or `person_attributes` (person-level)
3. Link via `run_id` FK
4. Update `evaluation_status` from `draft` → `reviewed` → `validated` as you check the outputs

See `db/migrate_10_load_career_tags.py` and `db/migrate_11_load_person_attributes.py` for implementation patterns.

New `attribute_name` values for `person_attributes` should be documented here when added.

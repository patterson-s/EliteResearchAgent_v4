# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A prosopography database and exploration platform for 75 members of four UN Secretary-General High-Level Panels (HLPs: 2004, 2007, 2012, 2020). Tracks elite career trajectories, org networks, and biographical data. Uses a **star schema** with persons at the center, radiating to career events, organizations, and analytical derivatives.

## Running the App

```bash
# Windows
serve.bat

# Cross-platform
uvicorn web.app:app --reload --host 127.0.0.1 --port 8000
```

- Main explorer: http://localhost:8000
- Ontology editor: http://localhost:8000/ontology-editor
- API docs: http://localhost:8000/docs

## Database Migrations

17 migration scripts in `db/` — safe to re-run (idempotent). Run in order from `migrate_01` through `migrate_17`. `resolve_parent_orgs.py` is a standalone helper (not a sequential migration).

Connection config comes from `.env`:
```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
```

**Schema prefix:** All tables live in the `prosopography` schema. Always use fully-qualified names: `prosopography.persons`, `prosopography.career_positions`, etc.

## Architecture

```
web/
  app.py          # FastAPI root; mounts 5 routers + static files
  db.py           # psycopg2 connection context manager (get_db_connection())
  models.py       # Pydantic response models for all endpoints
  routers/
    hlp.py        # GET /api/hlp/
    persons.py    # GET /api/persons/, /api/persons/{id}
    organizations.py  # GET /api/organizations/, /api/organizations/{id}
    search.py     # GET /api/search/
    ontology.py   # /api/ontology/* — annotation system (11 endpoints, most complex)
  static/
    index.html          # Main explorer UI (persons/orgs/search)
    ontology-editor.html  # Manual annotation workflow UI
db/
  migrate_*.py    # Sequential schema + data migrations (17 total)
  db_utils.py     # Shared DB connection for migration scripts
```

## Key Database Tables

**Core data:**
- `persons` (75 rows) — central entity
- `career_positions` (~2,183 rows) — one row per job; has `org_id` FK (93.8% matched)
- `organizations` (2,619 rows) — canonical org registry (1,609 base + 1,010 auto-stubs)
- `organization_aliases` (209 rows) — alternative names for org resolution

**Analytical derivatives (all provenance-tracked via `derivative_runs`):**
- `position_tags` (2,181 rows) — per-career-position domain/role/function tags
- `person_attributes` (414 rows) — person-level career typology attributes
- `org_ontology_mappings` (168 rows) — country-agnostic equivalence class annotations for orgs
- `ontology_user_classes` (36 rows) — user-defined L4+ equivalence class extensions

**Provenance tracking:** `derivative_runs` is the header table for all analytical runs. Every derivative row links to a `run_id`. Never insert derivatives without a corresponding `derivative_runs` row.

## Ontology System

The ontology editor annotates organizations into three categories (`mfa`, `executive`, `io_non_un`), each with a hardcoded hierarchy. Key fields in `org_ontology_mappings`:

- `equivalence_class` — country-agnostic role (e.g., `ministry_of_foreign_affairs`, `cabinet`)
- `country_code` — ISO alpha-3
- `hierarchy_path` — TEXT[] auto-computed from parent chain (e.g., `['national_government', 'ministry_of_foreign_affairs', 'embassy']`)
- `thematic_tags` — TEXT[] added in migrate_14
- `parent_org` — TEXT for org-to-org sub-unit relationships, added in migrate_15

Active runs: run_id=5 (MFA, reviewed), run_id=6 (executive, reviewed), run_id=8 (io_non_un, reviewed), run_id=10 (un_agencies, draft — in progress). Runs 1–4 are earlier career-tag/typology derivatives (not org ontology).

### Adding a New Annotation Category

Adding a category (e.g., `legislature`, `think_tank`) requires coordinated changes in **two files**:

1. **`web/routers/ontology.py`** — add to `CATEGORY_CONFIG` (equivalence classes, `candidate_where` SQL), `_DEFAULT_PARENT`, and `_GRANDPARENT` dicts.
2. **`web/static/ontology-editor.html`** — mirror the same additions in the JS `DEFAULT_PARENT`, `GRANDPARENT` dicts and `suggestLabel()` switch cases.
3. **Seed a `derivative_runs` row** (INSERT with `run_type='ontology_mapping'`, `entity_level='organization'`, scope_json `{"category": "..."}`) — then select it in the UI.

The executive queue additionally filters to orgs that have direct `career_positions.org_id` links (~52 "load-bearing" orgs). MFA queue does not apply this filter.

### Org Split Feature

`POST /api/ontology/orgs/{org_id}/split` — when an org has incompatible titles (e.g., "Swiss Confederation" linked to both President and Vice President), split creates new org rows, reassigns `career_positions.org_id`, and adds the new orgs to the queue. The original org retains any unassigned positions.

## Schema Reference

See `DATABASE.md` for full schema documentation (row counts, column definitions, useful queries) and `instructions.md` for project design philosophy. `pickup.md` has additional operational context for the ontology annotation workflow.

## Dependencies

```
psycopg2-binary>=2.9
python-dotenv>=1.0
fastapi>=0.111
uvicorn[standard]>=0.29
```

No test suite exists. Validation is done via migration integrity checks and manual UI review.

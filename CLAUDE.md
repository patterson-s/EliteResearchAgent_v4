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

All 15 migration scripts are in `db/` and are safe to re-run (idempotent). Run in order:

```bash
cd db/
python migrate_01_create_schema.py
# ... through ...
python migrate_15_parent_org.py
```

Connection config comes from `.env`:
```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
```

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
  migrate_*.py    # Sequential schema + data migrations (15 total)
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

Active runs: run_id=5 (MFA, reviewed), run_id=6 (executive, reviewed).

## Schema Reference

See `DATABASE.md` for full schema documentation and `instructions.md` for project design philosophy.

## Dependencies

```
psycopg2-binary>=2.9
python-dotenv>=1.0
fastapi>=0.111
uvicorn[standard]>=0.29
```

No test suite exists. Validation is done via migration integrity checks and manual UI review.

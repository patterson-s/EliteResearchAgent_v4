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
- Health check: http://localhost:8000/health

## Database Migrations

22 migration scripts in `db/` — safe to re-run (idempotent). Run in order from `migrate_01` through `migrate_22`. `resolve_parent_orgs.py` and `derive_functional_summary.py` are standalone helpers (not sequential migrations).

Connection config comes from `.env`:
```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
```

On Render, `DATABASE_URL` is set instead (postgres:// scheme auto-corrected to postgresql:// in `web/db.py`).

**Schema prefix:** All tables live in the `prosopography` schema. Always use fully-qualified names: `prosopography.persons`, `prosopography.career_positions`, etc.

## Architecture

```
web/
  app.py          # FastAPI root; mounts 6 routers + static files; BasicAuthMiddleware
  db.py           # get_conn() context manager; row_to_dict() / rows_to_dicts() helpers
  models.py       # Pydantic response models for all endpoints
  routers/
    hlp.py        # GET /api/hlp/
    persons.py    # GET /api/persons/, /api/persons/{id}
    organizations.py  # GET /api/organizations/, /api/organizations/{id}
    search.py     # GET /api/search/
    ontology.py   # /api/ontology/* — annotation system (11 endpoints, most complex)
    tags.py       # /api/tags/* — user functional tags on persons and positions
  static/
    index.html          # Main explorer UI (persons/orgs/search)
    ontology-editor.html  # Manual annotation workflow UI
    ontology-editor-v1.html  # Legacy version
db/
  migrate_*.py              # Sequential schema + data migrations (01–22)
  db_utils.py               # Shared DB connection for migration scripts (returns plain psycopg2 conn)
  enrich_org_locations.py   # Standalone: Serper + Cohere pipeline to populate org location fields
  generate_person_pdfs.py   # Standalone: generates static/person_pdfs/*.pdf for all 75 persons
  generate_org_pdf.py       # Standalone: generates static/org_pdfs/organizations.pdf (single doc, all corpus-linked orgs)
static/
  person_pdfs/  # 75 generated PDFs (one per person, named {id:03d}_{slug}.pdf)
  org_pdfs/     # organizations.pdf (single multi-section PDF, ordered by person count)
```

### Two DB connection patterns

**Web layer** (`web/db.py`): `get_conn()` is a context manager that opens and closes a connection per request. Routers use plain `conn.cursor()` and convert rows with `row_to_dict()` / `rows_to_dicts()`.

**Migration / script layer** (`db/db_utils.py`): `get_connection()` returns a raw psycopg2 connection (caller manages lifecycle). Scripts use `psycopg2.extras.RealDictCursor` so rows are addressable by column name directly.

## Key Database Tables

**Core data:**
- `persons` (75 rows) — central entity
- `career_positions` (~2,183 rows) — one row per job; has `org_id` FK (93.8% matched)
- `organizations` (2,619 rows) — canonical org registry (1,609 base + 1,010 auto-stubs); has `location_city`, `location_country`, `location_region` columns
- `organization_aliases` (209 rows) — alternative names for org resolution

**Analytical derivatives (all provenance-tracked via `derivative_runs`):**
- `position_tags` (2,181 rows) — per-career-position domain/role/function tags
- `person_attributes` (414 rows) — person-level career typology attributes
- `org_ontology_mappings` (168 rows) — country-agnostic equivalence class annotations for orgs
- `ontology_user_classes` (36 rows) — user-defined L4+ equivalence class extensions
- `org_location_searches` — Serper + Cohere location enrichment results per org per run (added migrate_22)

**User-applied tags (added in migrate_20/21):**
- `user_functional_tags` — one row per (entity_type, entity_id); tags stored as TEXT[]
- `functional_tag_vocab` — cumulative autocomplete vocabulary with use counts
- `person_notes` — free-text notes per person (added in migrate_21)

**Provenance tracking:** `derivative_runs` is the header table for all analytical runs. Every derivative row links to a `run_id`. Never insert derivatives without a corresponding `derivative_runs` row.

## Authentication

The app uses HTTP Basic Auth via `BasicAuthMiddleware` in `app.py`. Set env vars:
- `SITE_USERNAME` (default: `admin`)
- `SITE_PASSWORD` (empty = auth disabled)

On Render, these are set as non-synced env vars in `render.yaml`.

## Ontology System

The ontology editor annotates organizations into three categories (`mfa`, `executive`, `io_non_un`), each with a hardcoded hierarchy. Key fields in `org_ontology_mappings`:

- `equivalence_class` — country-agnostic role (e.g., `ministry_of_foreign_affairs`, `cabinet`)
- `country_code` — ISO alpha-3
- `hierarchy_path` — TEXT[] auto-computed from parent chain (e.g., `['national_government', 'ministry_of_foreign_affairs', 'embassy']`)
- `thematic_tags` — TEXT[] added in migrate_14
- `parent_org` — TEXT for org-to-org sub-unit relationships, added in migrate_15

Active runs: run_id=5 (MFA, reviewed), run_id=6 (executive, reviewed), run_id=8 (io_non_un, reviewed), run_id=10 (un_agencies, draft — in progress), run_id=14 (org_location_enrichment, reviewed — 1,421 orgs processed). Runs 1–4 are earlier career-tag/typology derivatives (not org ontology).

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
xhtml2pdf>=0.2.11
cohere>=5
requests>=2.31
```

`xhtml2pdf` is used only by the PDF generation scripts (pure Python, no external binaries). WeasyPrint was evaluated but requires GTK on Windows. `cohere` and `requests` are used only by `db/enrich_org_locations.py`. To regenerate:
- Person PDFs (75 files): `python db/generate_person_pdfs.py`
- Org PDF (single file): `python db/generate_org_pdf.py`
- Org locations (Serper + Cohere): `python db/enrich_org_locations.py` — resumes automatically if interrupted; use `--workers N` to tune parallelism; requires `SERPER_API_KEY` and `COHERE_API_KEY` in `.env`

No test suite exists. Validation is done via migration integrity checks and manual UI review.

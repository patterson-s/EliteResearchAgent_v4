# Pickup: Org Ontology Annotation — Session Summary

## What Was Built

A full manual annotation tool for building a derivative ontology layer over the `organizations` table. The tool maps existing orgs to hierarchical, country-agnostic equivalence classes without modifying source data.

**Running app:** `uvicorn web.app:app --reload` → http://127.0.0.1:8000
**Annotation UI:** http://127.0.0.1:8000/ontology-editor

---

## Database State

### Derivative Runs (org ontology)

| run_id | name | status | annotations |
|--------|------|--------|-------------|
| 5 | org_ontology_mfa_v1 | reviewed | 82 |
| 6 | org_ontology_executive_v1 | reviewed | 78 |

Runs 1–4 are earlier career-tag derivatives (not org ontology).

### Key Tables

| Table | Purpose |
|-------|---------|
| `prosopography.org_ontology_mappings` | All annotations; keyed on `(org_id, run_id)` |
| `prosopography.derivative_runs` | Provenance headers for each annotation pass |
| `prosopography.ontology_user_classes` | User-defined sub-classes created during executive run (36 entries) |
| `prosopography.organizations` | Source org table — never modified |

### Coverage Gap
**1,460 orgs** have career positions but no ontology annotation. These are candidates for future runs.

---

## Equivalence Class Hierarchy

### Hardcoded (in `web/routers/ontology.py` → `CATEGORY_CONFIG`)

**MFA run classes:**
- `national_government` (L1)
- `ministry_of_foreign_affairs` (L2)
- `embassy`, `permanent_mission`, `consulate`, `diplomatic_service` (L3)
- `not_mfa`, `needs_review` (exclude)

**Executive run classes:**
- `national_government` (L1)
- `executive_branch` (L2)
- `head_of_state`, `head_of_government`, `vice_head_of_state`, `cabinet`, `executive_office`, `national_security_council`, `executive_advisory`, `special_envoy` (L3)
- `presidential_campaign` (L0 — no hierarchy)
- `not_executive`, `needs_review` (exclude)

### User-Defined Sub-Classes (L4+, stored in DB)
Created during executive run. Examples:
- `ministry_of_finance`, `ministry_of_health` → parent: `cabinet`
- `national_security_advisor`, `arms_control`, `ai_strategy` → parent: `executive_advisory`
- `chief_of_staff`, `press_secretary`, `policy_advisor` → parent: `executive_office`

New sub-classes can be added live in the annotation UI via the "Sub-class" field.

---

## Architecture

### Key Files

| File | Role |
|------|------|
| `web/routers/ontology.py` | All API endpoints |
| `web/models.py` | Pydantic models for all ontology types |
| `web/static/ontology-editor.html` | Single-file annotation UI |
| `db/migrate_12_create_org_ontology.py` | Creates `org_ontology_mappings` + seeds run_id 5 & 6 |
| `db/migrate_13_user_classes.py` | Creates `ontology_user_classes` |

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/ontology/runs` | List all runs |
| GET | `/api/ontology/queue/{category}?run_id=` | Annotation queue for a category |
| GET | `/api/ontology/progress/{category}?run_id=` | Progress counts |
| GET | `/api/ontology/autocomplete/equivalence-classes?category=` | Hardcoded + user-defined classes |
| GET | `/api/ontology/autocomplete/countries` | Country codes |
| GET | `/api/ontology/orgs/{org_id}/context` | Career events for an org (3-tier cascade) |
| POST | `/api/ontology/mappings` | Save/update annotation |
| DELETE | `/api/ontology/mappings/{mapping_id}` | Remove annotation |
| POST | `/api/ontology/orgs/{org_id}/split` | Split one org into multiple by title |

### Queue Logic
Each category defines a `candidate_where` SQL fragment. The **executive queue** additionally requires `EXISTS (SELECT 1 FROM career_positions cp WHERE cp.org_id = o.org_id)` — only orgs with direct corpus career position links are shown (the ~52 "load-bearing" orgs). This was a deliberate decision: orgs without direct links came from classification tags and have no corpus members.

### Career Event Context Panel
Uses a 3-tier cascade:
1. Direct `cp.org_id = org_id` match
2. Phrase match: `cp.organization ILIKE '%canonical_name%'` (strips parenthetical variants first)
3. Sibling org lookup: other orgs in same country sharing keywords → shows their linked positions

Returns `match_type` field (`direct` / `approximate` / `sibling` / `none`) displayed as a note in the UI.

### Org Split Feature
When an org has multiple incompatible titles (e.g., President + Vice President both linked to "Swiss Confederation"), the "Split Org by Title" button creates new org entries, reassigns `career_positions.org_id`, and adds the new orgs to the queue. Original org retains any unassigned positions.

---

## Starting a New Annotation Run

To add a new category (e.g., legislature, think tank, UN bodies):

1. **Add to `CATEGORY_CONFIG`** in `web/routers/ontology.py`:
   - Define `equivalence_classes` list with value/label/level
   - Define `candidate_where` SQL fragment
   - Add new classes to `_DEFAULT_PARENT` and `_GRANDPARENT` dicts

2. **Add to JS constants** in `web/static/ontology-editor.html`:
   - `DEFAULT_PARENT` dict
   - `GRANDPARENT` dict
   - `suggestLabel()` switch cases

3. **Seed a new `derivative_runs` row** in the DB:
   ```sql
   INSERT INTO prosopography.derivative_runs
       (run_name, run_type, entity_level, evaluation_status, scope_json)
   VALUES
       ('org_ontology_legislature_v1', 'ontology_mapping', 'organization', 'draft',
        '{"category": "legislature"}');
   ```

4. Select the new run in the editor UI → queue populates automatically.

---

## Known Limitations / Future Work

- **1,460 unannotated orgs** with corpus career positions — future runs needed
- Some user-defined sub-classes from the executive run have inconsistent casing/naming (e.g., `'foreign affairs'` vs `'Ministry of Finance'`) — may want a cleanup pass
- Org deduplication: multiple org_ids for the same institution (e.g., Swiss Federal Council as org_id 471 and 685) — the split tool addresses new cases but legacy duplicates remain
- The `needs_review` category (5 entries in executive run) should be revisited

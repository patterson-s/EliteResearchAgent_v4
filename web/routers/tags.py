"""
Functional Tags router
----------------------
Endpoints for reading and writing user-applied free-form functional tags
at the person and career-position level.

Storage:
  prosopography.user_functional_tags  — one tag-set row per (entity_type, entity_id)
  prosopography.functional_tag_vocab  — cumulative autocomplete vocabulary
"""

from fastapi import APIRouter, HTTPException, Query
from web.db import get_conn
from web.models import FunctionalTagsItem, FunctionalTagsUpsertRequest, FunctionalTagVocabItem

router = APIRouter()


def _resolve_run_id(run_name: str) -> int:
    """Look up the run_id for a named derivative run."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT run_id FROM prosopography.derivative_runs WHERE run_name = %s",
            (run_name,),
        )
        row = cur.fetchone()
        cur.close()
    if row is None:
        raise RuntimeError(f"derivative_runs row '{run_name}' not found — run migrate_20 first.")
    return row[0]


# Resolved once at import time (after migration has run)
PERSON_RUN_ID: int = _resolve_run_id("user_ftags_person_v1")
POSITION_RUN_ID: int = _resolve_run_id("user_ftags_position_v1")

_RUN_ID_MAP = {"person": PERSON_RUN_ID, "position": POSITION_RUN_ID}


# ── Vocab ──────────────────────────────────────────────────────────────────────

@router.get("/vocab", response_model=list[FunctionalTagVocabItem])
def get_vocab(q: str = Query(default="", description="Prefix filter")):
    """Return known functional tags ordered by use_count descending (max 20)."""
    with get_conn() as conn:
        cur = conn.cursor()
        if q:
            cur.execute(
                """
                SELECT tag_name, use_count
                FROM prosopography.functional_tag_vocab
                WHERE tag_name ILIKE %(prefix)s
                ORDER BY use_count DESC, tag_name
                LIMIT 20
                """,
                {"prefix": f"{q}%"},
            )
        else:
            cur.execute(
                """
                SELECT tag_name, use_count
                FROM prosopography.functional_tag_vocab
                ORDER BY use_count DESC, tag_name
                LIMIT 20
                """
            )
        rows = cur.fetchall()
        cur.close()
    return [FunctionalTagVocabItem(tag_name=r[0], use_count=r[1]) for r in rows]


# ── Person tags ────────────────────────────────────────────────────────────────

@router.get("/person/{person_id}", response_model=FunctionalTagsItem)
def get_person_tags(person_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tags FROM prosopography.user_functional_tags
            WHERE entity_type = 'person' AND entity_id = %(eid)s
            """,
            {"eid": person_id},
        )
        row = cur.fetchone()
        cur.close()
    return FunctionalTagsItem(
        entity_type="person",
        entity_id=person_id,
        tags=row[0] if row else [],
    )


@router.put("/person/{person_id}", response_model=FunctionalTagsItem)
def put_person_tags(person_id: int, body: FunctionalTagsUpsertRequest):
    return _upsert_tags("person", person_id, body.tags)


# ── Position tags ──────────────────────────────────────────────────────────────

@router.get("/position/{position_id}", response_model=FunctionalTagsItem)
def get_position_tags(position_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tags FROM prosopography.user_functional_tags
            WHERE entity_type = 'position' AND entity_id = %(eid)s
            """,
            {"eid": position_id},
        )
        row = cur.fetchone()
        cur.close()
    return FunctionalTagsItem(
        entity_type="position",
        entity_id=position_id,
        tags=row[0] if row else [],
    )


@router.put("/position/{position_id}", response_model=FunctionalTagsItem)
def put_position_tags(position_id: int, body: FunctionalTagsUpsertRequest):
    return _upsert_tags("position", position_id, body.tags)


# ── Shared upsert logic ────────────────────────────────────────────────────────

def _upsert_tags(entity_type: str, entity_id: int, new_tags: list[str]) -> FunctionalTagsItem:
    run_id = _RUN_ID_MAP[entity_type]
    # Normalise: lowercase, strip whitespace, deduplicate preserving order
    cleaned: list[str] = []
    seen: set[str] = set()
    for t in new_tags:
        t = t.strip().lower()
        if t and t not in seen:
            cleaned.append(t)
            seen.add(t)

    with get_conn() as conn:
        cur = conn.cursor()

        # Fetch current tags to compute the diff for vocab increment
        cur.execute(
            """
            SELECT tags FROM prosopography.user_functional_tags
            WHERE entity_type = %(et)s AND entity_id = %(eid)s
            """,
            {"et": entity_type, "eid": entity_id},
        )
        row = cur.fetchone()
        existing_tags: set[str] = set(row[0]) if row else set()
        added_tags = [t for t in cleaned if t not in existing_tags]

        # Upsert the tag-set
        cur.execute(
            """
            INSERT INTO prosopography.user_functional_tags
                (entity_type, entity_id, run_id, tags, updated_at)
            VALUES (%(et)s, %(eid)s, %(rid)s, %(tags)s, now())
            ON CONFLICT (entity_type, entity_id)
            DO UPDATE SET tags = EXCLUDED.tags, updated_at = now()
            """,
            {"et": entity_type, "eid": entity_id, "rid": run_id, "tags": cleaned},
        )

        # Increment use_count for newly added tags
        if added_tags:
            cur.executemany(
                """
                INSERT INTO prosopography.functional_tag_vocab (tag_name, use_count)
                VALUES (%s, 1)
                ON CONFLICT (tag_name)
                DO UPDATE SET use_count = functional_tag_vocab.use_count + 1
                """,
                [(t,) for t in added_tags],
            )

        conn.commit()
        cur.close()

    return FunctionalTagsItem(entity_type=entity_type, entity_id=entity_id, tags=cleaned)

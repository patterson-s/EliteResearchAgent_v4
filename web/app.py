import base64
import os
import secrets

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from web.routers import persons, organizations, search, hlp, ontology, tags, locations
from web.db import get_conn

SITE_USERNAME = os.environ.get("SITE_USERNAME", "admin")
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "")

print(f"[auth] SITE_PASSWORD set: {bool(SITE_PASSWORD)}", flush=True)


class BasicAuthMiddleware:
    """Pure ASGI middleware — works regardless of Starlette version."""

    def __init__(self, app, **kwargs):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        if not SITE_PASSWORD:
            await self.app(scope, receive, send)
            return

        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        auth = headers.get(b"authorization", b"").decode("latin-1")

        if self._valid(auth):
            await self.app(scope, receive, send)
            return

        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"content-type", b"text/plain; charset=utf-8"],
                [b"www-authenticate", b'Basic realm="Prosopography Explorer"'],
                [b"content-length", b"12"],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b"Unauthorized",
            "more_body": False,
        })

    @staticmethod
    def _valid(auth: str) -> bool:
        try:
            scheme, creds = auth.split(" ", 1)
            if scheme.lower() != "basic":
                return False
            user, pw = base64.b64decode(creds).decode().split(":", 1)
            ok_u = secrets.compare_digest(user, SITE_USERNAME)
            ok_p = secrets.compare_digest(pw, SITE_PASSWORD)
            return ok_u and ok_p
        except Exception:
            return False


app = FastAPI(
    title="Prosopography Explorer",
    version="1.0.0",
    description="Read-only interface for exploring UN High-Level Panel elites and their career trajectories.",
)


@app.on_event("startup")
def run_startup_migrations():
    """Idempotent DDL — safe to run on every deploy."""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                ALTER TABLE prosopography.organizations
                    ADD COLUMN IF NOT EXISTS location_lat  DOUBLE PRECISION,
                    ADD COLUMN IF NOT EXISTS location_lng  DOUBLE PRECISION;
            """)
            conn.commit()
            cur.close()
        print("[startup] migrate_23 DDL applied (location_lat/lng columns ensured)", flush=True)
    except Exception as e:
        print(f"[startup] migration warning: {e}", flush=True)

app.add_middleware(BasicAuthMiddleware)

app.include_router(hlp.router,           prefix="/api/hlp",           tags=["HLP"])
app.include_router(persons.router,       prefix="/api/persons",       tags=["Persons"])
app.include_router(organizations.router, prefix="/api/organizations", tags=["Organizations"])
app.include_router(search.router,        prefix="/api/search",        tags=["Search"])
app.include_router(ontology.router,      prefix="/api/ontology",      tags=["Ontology"])
app.include_router(tags.router,          prefix="/api/tags",           tags=["Tags"])
app.include_router(locations.router,     prefix="/api/locations",      tags=["Locations"])

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/health", include_in_schema=False)
def health():
    return {"auth_enabled": bool(SITE_PASSWORD)}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/ontology-editor", include_in_schema=False)
def ontology_editor():
    return FileResponse(os.path.join(_static_dir, "ontology-editor.html"))


@app.get("/ontology-editor-v1", include_in_schema=False)
def ontology_editor_v1():
    return FileResponse(os.path.join(_static_dir, "ontology-editor-v1.html"))

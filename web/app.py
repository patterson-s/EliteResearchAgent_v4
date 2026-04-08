import base64
import os
import secrets

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from web.routers import persons, organizations, search, hlp, ontology, tags

SITE_USERNAME = os.environ.get("SITE_USERNAME", "admin")
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "")


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if not SITE_PASSWORD:
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        if self._valid(auth):
            return await call_next(request)
        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Prosopography Explorer"'},
        )

    def _valid(self, auth: str) -> bool:
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

app.add_middleware(BasicAuthMiddleware)

app.include_router(hlp.router,           prefix="/api/hlp",           tags=["HLP"])
app.include_router(persons.router,       prefix="/api/persons",       tags=["Persons"])
app.include_router(organizations.router, prefix="/api/organizations", tags=["Organizations"])
app.include_router(search.router,        prefix="/api/search",        tags=["Search"])
app.include_router(ontology.router,      prefix="/api/ontology",      tags=["Ontology"])
app.include_router(tags.router,          prefix="/api/tags",           tags=["Tags"])

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/ontology-editor", include_in_schema=False)
def ontology_editor():
    return FileResponse(os.path.join(_static_dir, "ontology-editor.html"))


@app.get("/ontology-editor-v1", include_in_schema=False)
def ontology_editor_v1():
    return FileResponse(os.path.join(_static_dir, "ontology-editor-v1.html"))

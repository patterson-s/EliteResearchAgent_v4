import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from web.routers import persons, organizations, search, hlp

app = FastAPI(
    title="Prosopography Explorer",
    version="1.0.0",
    description="Read-only interface for exploring UN High-Level Panel elites and their career trajectories.",
)

app.include_router(hlp.router,           prefix="/api/hlp",           tags=["HLP"])
app.include_router(persons.router,       prefix="/api/persons",       tags=["Persons"])
app.include_router(organizations.router, prefix="/api/organizations", tags=["Organizations"])
app.include_router(search.router,        prefix="/api/search",        tags=["Search"])

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(_static_dir, "index.html"))

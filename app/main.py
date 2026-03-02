from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.routers import charts
from app.routers import datasets
from app.routers import pages
from app.routers import query


from app.db.session import Base, engine
import app.models.dataset  # noqa: F401 — register models with Base
import app.models.query  # noqa: F401
import app.models.feedback  # noqa: F401


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="DataPilot",
    description="Natural language CSV/spreadsheet analysis tool",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(datasets.router)
app.include_router(query.router)
app.include_router(charts.router)
app.include_router(pages.router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.routers import charts
from app.routers import datasets
from app.routers import pages
from app.routers import query


from app.db.session import Base, engine
import app.models.dataset  # noqa: F401 — register models with Base
import app.models.query  # noqa: F401
import app.models.feedback  # noqa: F401
import app.models.visitor  # noqa: F401


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="DataPilot",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        if "server" in response.headers:
            del response.headers["server"]
        return response


app.add_middleware(SecurityHeadersMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

app.include_router(datasets.router)
app.include_router(query.router)
app.include_router(charts.router)
app.include_router(pages.router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

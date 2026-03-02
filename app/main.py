from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import datasets


@asynccontextmanager
async def lifespan(app: FastAPI):
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


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

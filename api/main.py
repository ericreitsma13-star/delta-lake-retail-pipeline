"""FastAPI application entry point for the retail pipeline API."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routers import ingest, sales
from src.config import get_spark_session


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise the shared SparkSession before accepting requests.

    The session is a module-level singleton in src/config.py — calling
    get_spark_session() here ensures it is warmed up once at startup and
    reused by all route handlers for the lifetime of the process.
    """
    get_spark_session(app_name="RetailPipelineAPI")
    yield
    # SparkSession.stop() intentionally omitted — uvicorn's SIGTERM handles shutdown.


app = FastAPI(
    title="Retail Pipeline API",
    description="Query gold layer tables and ingest new transactions.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(ingest.router)
app.include_router(sales.router)

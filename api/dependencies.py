"""FastAPI dependency: provides the shared SparkSession.

The session is initialised once during the application lifespan (see api/main.py)
and returned here for injection into route handlers.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import get_spark_session


def get_spark() -> SparkSession:
    """Return the singleton SparkSession created at application startup."""
    return get_spark_session()

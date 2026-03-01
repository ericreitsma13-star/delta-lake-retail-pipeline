"""Shared pytest fixtures for the Delta Lake retail pipeline test suite."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta extensions enabled for testing.

    Scoped to the session so all tests share a single SparkSession, which avoids
    expensive JVM startup per test module.
    """
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("RetailPipelineTests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled",
            "false",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()

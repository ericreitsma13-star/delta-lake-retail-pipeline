"""Shared pytest fixtures for the Delta Lake retail pipeline test suite."""

import uuid

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Create a local SparkSession with Delta and Iceberg extensions enabled for testing.

    Scoped to the session so all tests share a single SparkSession, which avoids
    expensive JVM startup per test module.  A session-scoped temp directory is used
    as the Iceberg warehouse so each test run starts with a clean slate.
    """
    iceberg_warehouse = str(tmp_path_factory.mktemp("iceberg_warehouse"))
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("RetailPipelineTests")
        .config(
            "spark.sql.extensions",
            (
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
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
        # Iceberg named catalog backed by the temp warehouse
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse)
    )
    session = configure_spark_with_delta_pip(
        builder,
        extra_packages=[_ICEBERG_PACKAGE],
    ).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="function")
def iceberg_db(spark):
    """Provide a unique Iceberg namespace per test for full isolation.

    Creates ``iceberg.test_<hex>`` before the test and drops it (CASCADE) on
    teardown so every test begins with an empty catalog namespace.

    Yields:
        str: The namespace name (e.g. ``"test_a1b2c3d4"``).
    """
    db_name = f"test_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{db_name}")
    yield db_name
    # Hadoop catalog does not support CASCADE — drop tables individually first.
    try:
        tables = spark.sql(f"SHOW TABLES IN iceberg.{db_name}").collect()
        for t in tables:
            spark.sql(f"DROP TABLE IF EXISTS iceberg.{db_name}.{t.tableName}")
    except Exception:
        pass
    spark.sql(f"DROP NAMESPACE IF EXISTS iceberg.{db_name}")

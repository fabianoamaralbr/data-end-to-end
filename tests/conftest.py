"""Shared pytest fixtures — provides a local PySpark session for unit tests.

Requirements: Java 11+ must be on PATH (PySpark dependency).
In CI, GitHub Actions installs Java via actions/setup-java.
Locally: `sudo apt install default-jdk` or install from https://adoptium.net
"""
import shutil
import subprocess
import sys

import pytest
from pyspark.sql import SparkSession


def _java_available() -> bool:
    return shutil.which("java") is not None


def pytest_collection_modifyitems(items):
    if not _java_available():
        skip = pytest.mark.skip(reason="Java not found on PATH — required for PySpark. Install Java 11+.")
        for item in items:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("financial_data_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()

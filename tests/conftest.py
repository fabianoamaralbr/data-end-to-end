"""Fixtures compartilhadas do pytest — fornece uma sessao PySpark local para os testes unitarios.

Requisitos: Java 11+ precisa estar no PATH (dependencia do PySpark).
No CI, o GitHub Actions instala o Java via actions/setup-java.
Localmente: `sudo apt install default-jdk` ou instale em https://adoptium.net
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
        skip = pytest.mark.skip(reason="Java nao encontrado no PATH — obrigatorio para o PySpark. Instale o Java 11+.")
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

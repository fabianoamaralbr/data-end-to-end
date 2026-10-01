"""Fixtures compartilhadas do pytest — sessao PySpark local com Delta Lake.

Requisitos: Java 11+ no PATH (dependencia do PySpark). No CI, o GitHub Actions
instala o Java via actions/setup-java.

Testes marcados com ``@pytest.mark.delta`` gravam tabelas Delta reais. No Windows,
escrever no filesystem local exige winutils (HADOOP_HOME/bin/winutils.exe); sem ele
esses testes sao pulados localmente e rodam no CI (Linux).
"""
import os
import shutil
import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _java_available() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


def _delta_writes_supported() -> bool:
    if sys.platform != "win32":
        return True
    return (Path(os.environ.get("HADOOP_HOME", "")) / "bin" / "winutils.exe").is_file()


def pytest_collection_modifyitems(items):
    if not _java_available():
        skip = pytest.mark.skip(reason="Java nao encontrado — obrigatorio para o PySpark. Instale o Java 11+.")
        for item in items:
            item.add_marker(skip)
    elif not _delta_writes_supported():
        skip = pytest.mark.skip(reason="Escrita Delta local no Windows exige HADOOP_HOME/winutils; roda no CI.")
        for item in items:
            if "delta" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("financial_data_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse.as_uri())
        .config("spark.ui.enabled", "false")
    )
    # Sem winutils o Spark nem inicializa com spark.jars.packages no Windows; como os
    # testes Delta ja estao pulados nesse caso, a sessao sobe sem o Delta.
    if _delta_writes_supported():
        builder = configure_spark_with_delta_pip(
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()

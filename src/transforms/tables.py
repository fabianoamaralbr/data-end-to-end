"""Criacao e verificacao das tabelas a partir dos contratos."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from transforms.contracts import validate_contract

# Synapse Serverless SQL le Delta apenas com reader v1: deletion vectors (reader
# feature, ligada por padrao em runtimes recentes do Databricks) tornariam as tabelas
# ilegiveis para o Synapse. Pelo mesmo motivo nao usamos liquid clustering — o layout
# e otimizado com OPTIMIZE ... ZORDER BY, que e transparente para qualquer leitor.
DEFAULT_PROPERTIES = {"delta.enableDeletionVectors": "false"}


def column_ddl(schema: StructType) -> str:
    return ",\n  ".join(
        f"`{f.name}` {f.dataType.simpleString()}" + ("" if f.nullable else " NOT NULL")
        for f in schema.fields
    )


def create_table_ddl(
    name: str,
    schema: StructType,
    *,
    location: str | None = None,
    properties: Mapping[str, str] | None = None,
) -> str:
    props = {**DEFAULT_PROPERTIES, **(properties or {})}
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(props.items()))
    location_sql = f"\nLOCATION '{location}'" if location else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {name} (\n  {column_ddl(schema)}\n)\n"
        f"USING DELTA{location_sql}\nTBLPROPERTIES ({props_sql})"
    )


def ensure_table(
    spark: SparkSession,
    name: str,
    schema: StructType,
    *,
    location: str | None = None,
    properties: Mapping[str, str] | None = None,
) -> None:
    """Cria a tabela se nao existir e falha se a existente divergir do contrato."""
    spark.sql(create_table_ddl(name, schema, location=location, properties=properties))
    validate_contract(spark.table(name).schema, schema, context=name)


def optimize(spark: SparkSession, name: str, zorder_by: Sequence[str]) -> None:
    spark.sql(f"OPTIMIZE {name} ZORDER BY ({', '.join(f'`{c}`' for c in zorder_by)})")

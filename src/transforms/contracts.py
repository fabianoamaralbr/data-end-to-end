"""Data contracts: o schema esperado de cada tabela, em um unico lugar.

Os mesmos StructType sao usados para:
- gerar o DDL das tabelas (``tables.create_table_ddl``);
- validar cada DataFrame antes de qualquer escrita (``conform``);
- validar o schema das tabelas ja existentes no setup (``tables.ensure_table``).

Nenhuma escrita usa ``mergeSchema``/``overwriteSchema``: qualquer mudanca de schema
falha explicitamente com ``SchemaContractError`` e exige mudanca versionada aqui.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SchemaContractError(Exception):
    """O schema observado nao corresponde ao contrato declarado."""


def _f(name: str, dtype, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)


# ── Metadados de ingestao (comuns a todas as tabelas bronze) ──────────────────
INGESTION_METADATA = [
    _f("_rescued_data", StringType()),
    _f("_source_file", StringType(), False),
    _f("_source_modified_at", TimestampType()),
    _f("_ingested_at", TimestampType(), False),
    _f("_run_id", StringType(), False),
    _f("_ingestion_key", StringType(), False),
]

# ── OHLCV (Kaggle) ────────────────────────────────────────────────────────────
OHLCV_BUSINESS_COLUMNS = ["Date", "Symbol", "Open", "High", "Low", "Close", "Volume", "OpenInt"]

# Raw e lido inteiramente como string: tipagem e responsabilidade da silver,
# e valores invalidos precisam chegar intactos na quarentena.
RAW_OHLCV = StructType([_f(c, StringType()) for c in OHLCV_BUSINESS_COLUMNS])

BRONZE_OHLCV = StructType(list(RAW_OHLCV.fields) + INGESTION_METADATA)

SILVER_OHLCV = StructType([
    _f("Date", DateType(), False),
    _f("Symbol", StringType(), False),
    _f("Open", DoubleType()),
    _f("High", DoubleType()),
    _f("Low", DoubleType()),
    _f("Close", DoubleType()),
    _f("Volume", LongType()),
    _f("OpenInt", LongType()),
    _f("daily_return_pct", DoubleType()),
    _f("intraday_range", DoubleType()),
    _f("price_spread_pct", DoubleType()),
    _f("_ingestion_key", StringType(), False),
    _f("_source_file", StringType(), False),
    _f("_ingested_at", TimestampType(), False),
    _f("_transformed_at", TimestampType(), False),
])

GOLD_OHLCV_DETAIL = StructType([
    _f("Date", DateType(), False),
    _f("Symbol", StringType(), False),
    _f("Open", DoubleType()),
    _f("High", DoubleType()),
    _f("Low", DoubleType()),
    _f("Close", DoubleType()),
    _f("Volume", LongType()),
    _f("daily_return_pct", DoubleType()),
    _f("intraday_range", DoubleType()),
    _f("price_spread_pct", DoubleType()),
    _f("ma_close_7d", DoubleType()),
    _f("ma_close_30d", DoubleType()),
    _f("ma_volume_7d", LongType()),
    _f("prev_close", DoubleType()),
    _f("day_over_day_return_pct", DoubleType()),
    _f("cumulative_return_pct", DoubleType()),
    _f("_gold_created_at", TimestampType(), False),
])

GOLD_OHLCV_DAILY_SUMMARY = StructType([
    _f("Symbol", StringType(), False),
    _f("Date", DateType(), False),
    _f("open", DoubleType()),
    _f("high", DoubleType()),
    _f("low", DoubleType()),
    _f("close", DoubleType()),
    _f("total_volume", LongType()),
    _f("daily_return_pct", DoubleType()),
    _f("ma_close_7d", DoubleType()),
    _f("ma_close_30d", DoubleType()),
    _f("day_over_day_return_pct", DoubleType()),
    _f("cumulative_return_pct", DoubleType()),
])

# ── Banco Central — SGS (fonte incremental) ───────────────────────────────────
SGS_BUSINESS_COLUMNS = ["series_code", "data", "valor"]

# A API devolve [{"data": "dd/MM/yyyy", "valor": "1.2345"}]; series_code vem do caminho.
RAW_SGS = StructType([_f("data", StringType()), _f("valor", StringType())])

BRONZE_SGS = StructType([_f(c, StringType()) for c in SGS_BUSINESS_COLUMNS] + INGESTION_METADATA)

SILVER_SGS = StructType([
    _f("series_code", IntegerType(), False),
    _f("series_name", StringType(), False),
    _f("ref_date", DateType(), False),
    _f("value", DecimalType(20, 8)),
    _f("_ingestion_key", StringType(), False),
    _f("_source_file", StringType(), False),
    _f("_ingested_at", TimestampType(), False),
    _f("_transformed_at", TimestampType(), False),
])

# ── Quarentena e metricas de qualidade ────────────────────────────────────────
_QUARANTINE_METADATA = [
    _f("_rejection_reason", StringType(), False),
    _f("_quarantined_at", TimestampType(), False),
]

QUARANTINE_OHLCV = StructType(list(BRONZE_OHLCV.fields) + _QUARANTINE_METADATA)
QUARANTINE_SGS = StructType(list(BRONZE_SGS.fields) + _QUARANTINE_METADATA)

DQ_METRICS = StructType([
    _f("table_name", StringType(), False),
    _f("batch_id", LongType(), False),
    _f("run_id", StringType(), False),
    _f("rows_in", LongType(), False),
    _f("rows_valid", LongType(), False),
    _f("rows_quarantined", LongType(), False),
    _f("rows_deduplicated", LongType(), False),
    _f("quarantine_rate", DoubleType(), False),
    _f("measured_at", TimestampType(), False),
])


# ── Validacao ─────────────────────────────────────────────────────────────────
def schema_diff(actual: StructType, expected: StructType) -> list[str]:
    """Lista as divergencias entre dois schemas (nomes e tipos; nulabilidade e ignorada).

    A nulabilidade fica a cargo das constraints NOT NULL das tabelas Delta, que
    rejeitam a escrita se um valor nulo chegar numa coluna obrigatoria.
    """
    actual_types = {f.name: f.dataType for f in actual.fields}
    expected_types = {f.name: f.dataType for f in expected.fields}

    problems = [f"coluna ausente: {name}" for name in expected_types if name not in actual_types]
    problems += [f"coluna inesperada: {name}" for name in actual_types if name not in expected_types]
    problems += [
        f"tipo divergente em {name}: esperado {dtype.simpleString()}, "
        f"recebido {actual_types[name].simpleString()}"
        for name, dtype in expected_types.items()
        if name in actual_types and actual_types[name] != dtype
    ]
    return problems


def validate_contract(actual: StructType, expected: StructType, *, context: str) -> None:
    problems = schema_diff(actual, expected)
    if problems:
        raise SchemaContractError(f"[{context}] schema fora do contrato: " + "; ".join(problems))


def conform(df: DataFrame, contract: StructType, *, context: str) -> DataFrame:
    """Valida o DataFrame contra o contrato e devolve as colunas na ordem do contrato."""
    validate_contract(df.schema, contract, context=context)
    return df.select(*contract.fieldNames())

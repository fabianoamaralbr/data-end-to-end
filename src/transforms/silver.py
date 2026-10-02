"""Camada silver OHLCV: tipagem, validacao com quarentena, deduplicacao deterministica e metricas."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transforms.contracts import (
    BRONZE_OHLCV,
    QUARANTINE_OHLCV,
    SILVER_OHLCV,
    conform,
)
from transforms.quality import Rule, split_valid_quarantine

OHLCV_KEYS = ("Symbol", "Date")

# Criterio de "versao mais recente" de um registro. A ultima coluna (hash do conteudo)
# garante desempate total: o resultado nao depende da ordem fisica das linhas.
LATEST_ORDER = ("_ingested_at", "_source_file", "_ingestion_key")


@dataclass(frozen=True)
class SilverResult:
    valid: DataFrame
    quarantine: DataFrame


def deduplicate_latest(df: DataFrame, keys: Sequence[str], order_cols: Sequence[str] = LATEST_ORDER) -> DataFrame:
    """Mantem exatamente um registro por chave: o mais recente segundo ``order_cols``.

    Diferente de ``dropDuplicates``, o sobrevivente e definido explicitamente e e o
    mesmo em qualquer execucao.
    """
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc() for c in order_cols])
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def cast_ohlcv(df: DataFrame) -> DataFrame:
    """Tipa as colunas de negocio em colunas ``_t_*``, preservando os valores brutos.

    ``try_cast``/``try_to_timestamp`` devolvem NULL em vez de falhar (inclusive com
    ANSI mode ligado), e os valores originais seguem intactos para a quarentena.
    """
    return (
        df
        .withColumn("_t_Date", F.expr("to_date(try_to_timestamp(Date, 'yyyy-MM-dd'))"))
        .withColumn("_t_Symbol", F.upper(F.trim(F.col("Symbol"))))
        .withColumn("_t_Open", F.expr("try_cast(Open AS DOUBLE)"))
        .withColumn("_t_High", F.expr("try_cast(High AS DOUBLE)"))
        .withColumn("_t_Low", F.expr("try_cast(Low AS DOUBLE)"))
        .withColumn("_t_Close", F.expr("try_cast(Close AS DOUBLE)"))
        .withColumn("_t_Volume", F.expr("try_cast(Volume AS BIGINT)"))
        .withColumn("_t_OpenInt", F.expr("try_cast(OpenInt AS BIGINT)"))
    )


def ohlcv_rules() -> tuple[Rule, ...]:
    return (
        Rule("schema_drift", F.col("_rescued_data").isNotNull()),
        Rule("date_invalid", F.col("_t_Date").isNull()),
        Rule("symbol_missing", F.col("_t_Symbol").isNull() | (F.col("_t_Symbol") == "")),
        Rule("open_invalid", F.col("_t_Open").isNull() | (F.col("_t_Open") <= 0)),
        Rule("close_invalid", F.col("_t_Close").isNull() | (F.col("_t_Close") <= 0)),
        Rule("high_invalid", F.col("_t_High").isNull() | (F.col("_t_High") <= 0)),
        Rule("low_invalid", F.col("_t_Low").isNull() | (F.col("_t_Low") <= 0)),
        Rule("high_below_low", F.col("_t_High") < F.col("_t_Low")),
        # A maxima/minima do dia precisam conter abertura e fechamento; o contrario indica
        # colunas trocadas ou erro de escala na origem (ex.: High em centavos).
        Rule(
            "ohlc_inconsistent",
            (F.col("_t_High") < F.greatest("_t_Open", "_t_Close"))
            | (F.col("_t_Low") > F.least("_t_Open", "_t_Close")),
        ),
        Rule("volume_invalid", F.col("_t_Volume").isNull() | (F.col("_t_Volume") < 0)),
    )


def add_derived_metrics(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("intraday_return_pct", F.round((F.col("Close") - F.col("Open")) / F.col("Open") * 100, 4))
        .withColumn("intraday_range", F.round(F.col("High") - F.col("Low"), 4))
        .withColumn("price_spread_pct", F.round((F.col("High") - F.col("Low")) / F.col("Open") * 100, 4))
    )


def to_silver(bronze_df: DataFrame) -> SilverResult:
    """Bronze OHLCV -> (silver valida e deduplicada, quarentena com motivo)."""
    bronze_df = conform(bronze_df, BRONZE_OHLCV, context="bronze.ohlcv")
    typed = cast_ohlcv(bronze_df)
    valid, rejected = split_valid_quarantine(typed, ohlcv_rules())

    typed_cols = [c for c in typed.columns if c.startswith("_t_")]
    valid = valid.drop(*[c[3:] for c in typed_cols])
    for c in typed_cols:
        valid = valid.withColumnRenamed(c, c[3:])

    silver = (
        add_derived_metrics(deduplicate_latest(valid, OHLCV_KEYS))
        .withColumn("_transformed_at", F.current_timestamp())
        .select(*SILVER_OHLCV.fieldNames())
    )
    return SilverResult(
        valid=conform(silver, SILVER_OHLCV, context="silver.ohlcv"),
        quarantine=conform(rejected.drop(*typed_cols), QUARANTINE_OHLCV, context="quarantine.ohlcv"),
    )

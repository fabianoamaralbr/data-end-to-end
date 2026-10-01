"""Processamento de cada micro-batch (``foreachBatch``) das tres camadas.

Os notebooks apenas montam o stream (Auto Loader / Change Data Feed) e delegam a
estas funcoes, que sao testadas com tabelas Delta reais em ``tests/``.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transforms.bronze import add_ingestion_metadata
from transforms.contracts import conform
from transforms.datasets import Dataset
from transforms.delta_ops import merge_insert_new, merge_replace_slice, merge_upsert_latest
from transforms.gold import GOLD_KEYS, build_daily_summary, build_detail
from transforms.quality import BatchMetrics, check_quarantine_rate
from transforms.silver import LATEST_ORDER


def process_bronze_batch(
    batch_df: DataFrame, batch_id: int, *, dataset: Dataset, target: DeltaTable, run_id: str
) -> None:
    """Append idempotente: MERGE insert-only pela chave de ingestao."""
    bronze = add_ingestion_metadata(
        dataset.prepare_bronze(batch_df), business_columns=dataset.business_columns, run_id=run_id
    )
    merge_insert_new(target, conform(bronze, dataset.bronze.contract, context=dataset.bronze.name), ["_ingestion_key"])


def process_silver_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    dataset: Dataset,
    silver: DeltaTable,
    quarantine: DeltaTable,
    metrics: DeltaTable,
    run_id: str,
    max_quarantine_rate: float,
) -> BatchMetrics:
    """Valida, separa a quarentena, registra metricas e faz upsert na silver.

    Ordem importa: quarentena e metricas sao gravadas *antes* do circuit breaker, para
    que um batch reprovado deixe rastro consultavel; a silver so e tocada se o batch passar.
    """
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    result = dataset.to_silver(batch_df)
    valid = result.valid.persist()
    rejected = result.quarantine.persist()
    try:
        batch_metrics = BatchMetrics(
            table_name=dataset.silver.name,
            batch_id=batch_id,
            run_id=run_id,
            rows_in=batch_df.count(),
            rows_valid=valid.count(),
            rows_quarantined=rejected.count(),
        )
        merge_insert_new(quarantine, rejected, ["_ingestion_key"])
        merge_upsert_latest(
            metrics, batch_metrics.to_df(spark), ["table_name", "run_id", "batch_id"], ["measured_at"]
        )
        check_quarantine_rate(batch_metrics, max_quarantine_rate)
        merge_upsert_latest(silver, valid, dataset.keys, LATEST_ORDER)
        return batch_metrics
    finally:
        for df in (batch_df, valid, rejected):
            df.unpersist()


def changed_symbols(changes_df: DataFrame) -> list[str]:
    """Simbolos com insert/update no Change Data Feed da silver."""
    rows = (
        changes_df
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .select("Symbol")
        .distinct()
        .collect()
    )
    return sorted(r["Symbol"] for r in rows)


def process_gold_batch(
    changes_df: DataFrame,
    batch_id: int,
    *,
    silver_table: str,
    detail: DeltaTable,
    summary: DeltaTable,
) -> list[str]:
    """Recalcula apenas os simbolos afetados e substitui suas fatias na gold."""
    symbols = changed_symbols(changes_df)
    if not symbols:
        return []

    spark = changes_df.sparkSession
    detail_df = build_detail(spark.table(silver_table).filter(F.col("Symbol").isin(symbols))).persist()
    try:
        merge_replace_slice(detail, detail_df, GOLD_KEYS, "Symbol", symbols)
        merge_replace_slice(summary, build_daily_summary(detail_df), GOLD_KEYS, "Symbol", symbols)
    finally:
        detail_df.unpersist()
    return symbols

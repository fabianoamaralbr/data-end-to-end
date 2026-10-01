"""Camada gold OHLCV: medias moveis, retornos com lag e resumo diario."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transforms.contracts import GOLD_OHLCV_DAILY_SUMMARY, GOLD_OHLCV_DETAIL, conform

GOLD_KEYS = ("Symbol", "Date")


def build_detail(silver_df: DataFrame) -> DataFrame:
    """Uma linha por (Symbol, Date) com janelas calculadas sobre o historico do simbolo.

    As janelas dependem do historico completo do simbolo, por isso o job gold recalcula
    o simbolo inteiro sempre que qualquer data dele muda na silver.
    """
    by_symbol = Window.partitionBy("Symbol").orderBy("Date")
    last_7 = by_symbol.rowsBetween(-6, Window.currentRow)
    last_30 = by_symbol.rowsBetween(-29, Window.currentRow)
    since_start = by_symbol.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    detail = (
        silver_df
        .withColumn("ma_close_7d", F.round(F.avg("Close").over(last_7), 4))
        .withColumn("ma_close_30d", F.round(F.avg("Close").over(last_30), 4))
        .withColumn("ma_volume_7d", F.round(F.avg("Volume").over(last_7), 0).cast("long"))
        .withColumn("prev_close", F.lag("Close", 1).over(by_symbol))
        .withColumn(
            "day_over_day_return_pct",
            F.round((F.col("Close") - F.col("prev_close")) / F.col("prev_close") * 100, 4),
        )
        .withColumn("_first_close", F.first("Close").over(since_start))
        .withColumn(
            "cumulative_return_pct",
            F.round((F.col("Close") - F.col("_first_close")) / F.col("_first_close") * 100, 4),
        )
        .withColumn("_gold_created_at", F.current_timestamp())
        .select(*GOLD_OHLCV_DETAIL.fieldNames())
    )
    return conform(detail, GOLD_OHLCV_DETAIL, context="gold.ohlcv_detail")


def build_daily_summary(detail_df: DataFrame) -> DataFrame:
    """Projecao do detalhe para o modelo do Power BI.

    A silver garante uma linha por (Symbol, Date), entao nao ha agregacao a fazer —
    a versao anterior agrupava com ``first()``, que e nao deterministico.
    """
    summary = detail_df.select(
        "Symbol",
        "Date",
        F.col("Open").alias("open"),
        F.col("High").alias("high"),
        F.col("Low").alias("low"),
        F.col("Close").alias("close"),
        F.col("Volume").alias("total_volume"),
        "daily_return_pct",
        "ma_close_7d",
        "ma_close_30d",
        "day_over_day_return_pct",
        "cumulative_return_pct",
    )
    return conform(summary, GOLD_OHLCV_DAILY_SUMMARY, context="gold.ohlcv_daily_summary")

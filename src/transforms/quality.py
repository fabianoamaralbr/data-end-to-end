"""Regras de rejeicao, quarentena e metricas de qualidade por batch."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from transforms.contracts import DQ_METRICS


class DataQualityError(Exception):
    """A taxa de rejeicao de um batch passou do limite aceitavel."""


@dataclass(frozen=True)
class Rule:
    """Regra de rejeicao: ``invalid_when`` verdadeiro => registro vai para a quarentena."""

    reason: str
    invalid_when: Column


def with_rejection_reason(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Adiciona ``_rejection_reason`` com todos os motivos violados, separados por ``;``.

    String vazia significa registro valido. ``coalesce(..., False)`` garante que uma
    regra avaliada como NULL nao reprova nem aprova por acidente.
    """
    reasons = [F.when(F.coalesce(rule.invalid_when, F.lit(False)), F.lit(rule.reason)) for rule in rules]
    return df.withColumn("_rejection_reason", F.concat_ws(";", *reasons))


def split_valid_quarantine(df: DataFrame, rules: Sequence[Rule]) -> tuple[DataFrame, DataFrame]:
    flagged = with_rejection_reason(df, rules)
    valid = flagged.filter(F.col("_rejection_reason") == "").drop("_rejection_reason")
    quarantine = (
        flagged
        .filter(F.col("_rejection_reason") != "")
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return valid, quarantine


@dataclass(frozen=True)
class BatchMetrics:
    table_name: str
    batch_id: int
    run_id: str
    rows_in: int
    rows_valid: int
    rows_quarantined: int

    @property
    def rows_deduplicated(self) -> int:
        return self.rows_in - self.rows_quarantined - self.rows_valid

    @property
    def quarantine_rate(self) -> float:
        return self.rows_quarantined / self.rows_in if self.rows_in else 0.0

    def to_df(self, spark: SparkSession) -> DataFrame:
        row = (
            self.table_name, self.batch_id, self.run_id, self.rows_in, self.rows_valid,
            self.rows_quarantined, self.rows_deduplicated, self.quarantine_rate,
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
        return spark.createDataFrame([row], DQ_METRICS)


def check_quarantine_rate(metrics: BatchMetrics, max_rate: float) -> None:
    """Circuit breaker: interrompe o pipeline antes de publicar um batch degradado."""
    if metrics.quarantine_rate > max_rate:
        raise DataQualityError(
            f"[{metrics.table_name}] batch {metrics.batch_id}: {metrics.rows_quarantined}/"
            f"{metrics.rows_in} registros em quarentena ({metrics.quarantine_rate:.2%}) "
            f"> limite {max_rate:.2%}. Consulte a tabela de quarentena."
        )

"""Banco Central do Brasil — SGS (Sistema Gerenciador de Series Temporais).

Fonte incremental: o ADF busca diariamente uma janela movel de 7 dias por serie
(cobre fins de semana, feriados e revisoes do BCB) e grava em
``raw/bcb_sgs/series_code=<codigo>/<data da janela>.json``. As janelas se sobrepoem
de proposito; o MERGE em (series_code, ref_date) torna a sobreposicao inofensiva.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transforms.contracts import BRONZE_SGS, QUARANTINE_SGS, SILVER_SGS, conform
from transforms.quality import Rule, split_valid_quarantine
from transforms.silver import SilverResult, deduplicate_latest

SGS_KEYS = ("series_code", "ref_date")

# Codigo SGS -> nome canonico. Serie fora do catalogo vai para a quarentena.
SERIES = {
    1: "usd_brl_ptax_venda",
    11: "selic_diaria",
    12: "cdi_diario",
    432: "selic_meta",
}


def with_series_code(df: DataFrame) -> DataFrame:
    """Extrai o codigo da serie do caminho ``.../series_code=<codigo>/arquivo.json``."""
    code = F.regexp_extract(F.col("_source_file"), r"series_code=(\d+)", 1)
    return df.withColumn("series_code", F.when(code != "", code))


def _series_name_map():
    return F.create_map(*[x for code, name in SERIES.items() for x in (F.lit(code), F.lit(name))])


def sgs_rules() -> tuple[Rule, ...]:
    return (
        Rule("schema_drift", F.col("_rescued_data").isNotNull()),
        Rule("series_unknown", F.col("_t_series_name").isNull()),
        Rule("date_invalid", F.col("_t_ref_date").isNull()),
        Rule("value_invalid", F.col("_t_value").isNull()),
    )


def to_silver(bronze_df: DataFrame) -> SilverResult:
    bronze_df = conform(bronze_df, BRONZE_SGS, context="bronze.bcb_sgs")
    typed = (
        bronze_df
        .withColumn("_t_series_code", F.expr("try_cast(series_code AS INT)"))
        .withColumn("_t_series_name", _series_name_map()[F.col("_t_series_code")])
        .withColumn("_t_ref_date", F.expr("to_date(try_to_timestamp(data, 'dd/MM/yyyy'))"))
        .withColumn("_t_value", F.expr("try_cast(valor AS DECIMAL(20, 8))"))
    )
    valid, rejected = split_valid_quarantine(typed, sgs_rules())
    typed_cols = ["_t_series_code", "_t_series_name", "_t_ref_date", "_t_value"]

    silver = (
        valid.select(
            F.col("_t_series_code").alias("series_code"),
            F.col("_t_series_name").alias("series_name"),
            F.col("_t_ref_date").alias("ref_date"),
            F.col("_t_value").alias("value"),
            "_ingestion_key",
            "_source_file",
            "_ingested_at",
        )
    )
    silver = deduplicate_latest(silver, SGS_KEYS).withColumn("_transformed_at", F.current_timestamp())
    return SilverResult(
        valid=conform(silver, SILVER_SGS, context="silver.bcb_sgs"),
        quarantine=conform(rejected.drop(*typed_cols), QUARANTINE_SGS, context="quarantine.bcb_sgs"),
    )

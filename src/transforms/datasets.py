"""Registro dos datasets: onde cada um vive e quais funcoes o transformam.

Os notebooks sao genericos e recebem apenas o nome do dataset; tudo que e
especifico de cada fonte esta aqui (e coberto por testes).
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from transforms import contracts as c
from transforms import sgs, silver
from transforms.silver import SilverResult


def abfss(storage_account: str, container: str, path: str = "") -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"


@dataclass(frozen=True)
class TableSpec:
    name: str          # <schema>.<tabela>; o catalogo e prefixado em tempo de execucao
    container: str
    path: str
    contract: StructType
    properties: Mapping[str, str] = field(default_factory=dict)
    zorder_by: tuple[str, ...] = ()

    def full_name(self, catalog: str) -> str:
        return f"{catalog}.{self.name}"

    def location(self, storage_account: str) -> str:
        return abfss(storage_account, self.container, self.path)


@dataclass(frozen=True)
class Dataset:
    name: str
    raw_path: str
    raw_format: str
    raw_options: Mapping[str, str]
    raw_schema: StructType
    business_columns: tuple[str, ...]
    keys: tuple[str, ...]
    bronze: TableSpec
    silver: TableSpec
    quarantine: TableSpec
    to_silver: Callable[[DataFrame], SilverResult]
    prepare_bronze: Callable[[DataFrame], DataFrame] = lambda df: df

    def checkpoint(self, storage_account: str, layer: str) -> str:
        return abfss(storage_account, layer, f"_checkpoints/{self.name}")


# Silver com Change Data Feed: o job gold le apenas o que mudou.
_SILVER_PROPS = {"delta.enableChangeDataFeed": "true"}

OHLCV = Dataset(
    name="ohlcv",
    raw_path="financial-data",
    raw_format="csv",
    raw_options={"header": "true"},
    raw_schema=c.RAW_OHLCV,
    business_columns=tuple(c.OHLCV_BUSINESS_COLUMNS),
    keys=silver.OHLCV_KEYS,
    bronze=TableSpec("bronze.ohlcv", "bronze", "financial_data", c.BRONZE_OHLCV),
    silver=TableSpec(
        "silver.ohlcv", "silver", "financial_data", c.SILVER_OHLCV,
        properties=_SILVER_PROPS, zorder_by=("Symbol", "Date"),
    ),
    quarantine=TableSpec("silver.ohlcv_quarantine", "silver", "_quarantine/financial_data", c.QUARANTINE_OHLCV),
    to_silver=silver.to_silver,
)

BCB_SGS = Dataset(
    name="bcb_sgs",
    raw_path="bcb_sgs",
    raw_format="json",
    # A API devolve um array JSON por arquivo. series_code e extraido do caminho por
    # sgs.with_series_code, entao a inferencia de particoes do Auto Loader fica desligada.
    raw_options={"multiLine": "true", "cloudFiles.partitionColumns": ""},
    raw_schema=c.RAW_SGS,
    business_columns=tuple(c.SGS_BUSINESS_COLUMNS),
    keys=sgs.SGS_KEYS,
    bronze=TableSpec("bronze.bcb_sgs", "bronze", "bcb_sgs", c.BRONZE_SGS),
    silver=TableSpec(
        "silver.bcb_sgs", "silver", "bcb_sgs", c.SILVER_SGS,
        properties=_SILVER_PROPS, zorder_by=("series_code", "ref_date"),
    ),
    quarantine=TableSpec("silver.bcb_sgs_quarantine", "silver", "_quarantine/bcb_sgs", c.QUARANTINE_SGS),
    to_silver=sgs.to_silver,
    prepare_bronze=sgs.with_series_code,
)

DATASETS = {d.name: d for d in (OHLCV, BCB_SGS)}

GOLD_OHLCV_DETAIL = TableSpec(
    "gold.ohlcv_detail", "gold", "financial_data", c.GOLD_OHLCV_DETAIL, zorder_by=("Symbol", "Date"),
)
GOLD_OHLCV_DAILY_SUMMARY = TableSpec(
    "gold.ohlcv_daily_summary", "gold", "daily_summary", c.GOLD_OHLCV_DAILY_SUMMARY,
    zorder_by=("Symbol", "Date"),
)
DQ_METRICS = TableSpec("ops.dq_metrics", "silver", "_ops/dq_metrics", c.DQ_METRICS)

ALL_TABLES = (
    *(t for d in DATASETS.values() for t in (d.bronze, d.silver, d.quarantine)),
    GOLD_OHLCV_DETAIL,
    GOLD_OHLCV_DAILY_SUMMARY,
    DQ_METRICS,
)


def get_dataset(name: str) -> Dataset:
    try:
        return DATASETS[name]
    except KeyError:
        raise ValueError(f"dataset desconhecido: {name!r}. Opcoes: {sorted(DATASETS)}") from None

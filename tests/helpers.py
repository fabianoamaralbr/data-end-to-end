"""Fabricas de DataFrames no formato que cada camada recebe em producao."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from transforms.bronze import add_ingestion_metadata
from transforms.contracts import OHLCV_BUSINESS_COLUMNS, RAW_OHLCV, RAW_SGS, SGS_BUSINESS_COLUMNS
from transforms.sgs import with_series_code

_AUTOLOADER_COLUMNS = [
    StructField("_rescued_data", StringType()),
    StructField("_source_file", StringType()),
    StructField("_source_modified_at", TimestampType()),
]

RAW_FILE = "abfss://raw@st.dfs.core.windows.net/financial-data/financial_data.csv"


def ohlcv_row(date="2024-01-02", symbol="AAPL", open_="100.0", high="105.0", low="99.0",
              close="104.0", volume="1000000", open_int="0") -> dict:
    return {"Date": date, "Symbol": symbol, "Open": open_, "High": high, "Low": low,
            "Close": close, "Volume": volume, "OpenInt": open_int}


def autoloader_batch(spark: SparkSession, rows: list[dict], raw_schema: StructType = RAW_OHLCV,
                     source_file: str = RAW_FILE, rescued: str | None = None) -> DataFrame:
    """O que o Auto Loader entrega ao foreachBatch: colunas raw + ``_rescued_data`` + ``_metadata``."""
    schema = StructType(list(raw_schema.fields) + _AUTOLOADER_COLUMNS)
    modified = datetime(2024, 1, 1)
    data = [tuple(r.get(f.name) for f in raw_schema.fields) + (rescued, source_file, modified) for r in rows]
    return spark.createDataFrame(data, schema)


def bronze_ohlcv(spark: SparkSession, rows: list[dict], *, ingested_at: str = "2024-01-03 06:00:00",
                 source_file: str = RAW_FILE, rescued: str | None = None) -> DataFrame:
    """Bronze OHLCV com ``_ingested_at`` fixo, para controlar a ordem de versoes nos testes."""
    batch = autoloader_batch(spark, rows, source_file=source_file, rescued=rescued)
    return (
        add_ingestion_metadata(batch, business_columns=OHLCV_BUSINESS_COLUMNS, run_id="test-run")
        .withColumn("_ingested_at", F.to_timestamp(F.lit(ingested_at)))
    )


def bronze_sgs(spark: SparkSession, rows: list[dict], *, series_code: int = 432,
               ingested_at: str = "2024-01-03 06:00:00", window: str = "2024-01-03") -> DataFrame:
    source = f"abfss://raw@st.dfs.core.windows.net/bcb_sgs/series_code={series_code}/{window}.json"
    batch = with_series_code(autoloader_batch(spark, rows, raw_schema=RAW_SGS, source_file=source))
    return (
        add_ingestion_metadata(batch, business_columns=SGS_BUSINESS_COLUMNS, run_id="test-run")
        .withColumn("_ingested_at", F.to_timestamp(F.lit(ingested_at)))
    )

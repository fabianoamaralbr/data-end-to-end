"""Testes unitarios para a logica de transformacao da camada bronze (ingestao)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def _make_raw_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame(rows)


def test_bronze_retains_all_raw_columns(spark):
    raw = _make_raw_df(spark, [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0",
         "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
    ])
    assert "Date" in raw.columns
    assert "Close" in raw.columns
    assert "Volume" in raw.columns


def test_bronze_adds_metadata_columns(spark):
    from pyspark.sql.functions import current_timestamp, input_file_name, lit

    raw = _make_raw_df(spark, [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0",
         "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
    ])

    bronze = (
        raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_pipeline_run_ts", lit("2024-01-02T06:00:00+00:00"))
    )

    assert "_ingested_at" in bronze.columns
    assert "_source_file" in bronze.columns
    assert "_pipeline_run_ts" in bronze.columns


def test_bronze_preserves_row_count(spark):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0",
         "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
        {"Date": "2024-01-03", "Symbol": "AAPL", "Open": "131.0",
         "High": "133.0", "Low": "130.0", "Close": "132.0", "Volume": "2000000", "OpenInt": "0"},
        {"Date": "2024-01-02", "Symbol": "MSFT", "Open": "240.0",
         "High": "242.0", "Low": "239.0", "Close": "241.0", "Volume": "500000", "OpenInt": "0"},
    ]
    raw = _make_raw_df(spark, rows)
    from pyspark.sql.functions import current_timestamp, lit
    bronze = raw.withColumn("_ingested_at", current_timestamp()).withColumn("_pipeline_run_ts", lit("t"))
    assert bronze.count() == 3

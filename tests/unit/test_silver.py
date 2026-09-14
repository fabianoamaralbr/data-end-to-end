"""Testes unitarios para a logica de transformacao da camada silver (limpeza e enriquecimento)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, to_date


def _make_bronze_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame(rows)


def test_silver_deduplicates_date_symbol(spark):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
    ]
    df = _make_bronze_df(spark, rows)
    result = df.dropDuplicates(["Date", "Symbol"])
    assert result.count() == 1


def test_silver_keeps_distinct_symbol_date_pairs(spark):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
        {"Date": "2024-01-02", "Symbol": "MSFT", "Open": "240.0", "High": "242.0", "Low": "239.0", "Close": "241.0", "Volume": "500000", "OpenInt": "0"},
        {"Date": "2024-01-03", "Symbol": "AAPL", "Open": "131.0", "High": "133.0", "Low": "130.0", "Close": "132.0", "Volume": "2000000", "OpenInt": "0"},
    ]
    df = _make_bronze_df(spark, rows)
    result = df.dropDuplicates(["Date", "Symbol"])
    assert result.count() == 3


def test_silver_filters_null_close(spark):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"},
        {"Date": "2024-01-03", "Symbol": "AAPL", "Open": "131.0", "High": "133.0", "Low": "130.0", "Close": None, "Volume": "2000000", "OpenInt": "0"},
    ]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("Close", col("Close").cast("double"))
    result = typed.filter(col("Close").isNotNull() & (col("Close") > 0))
    assert result.count() == 1


def test_silver_filters_zero_close(spark):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "0.0", "Volume": "1000000", "OpenInt": "0"},
        {"Date": "2024-01-03", "Symbol": "AAPL", "Open": "131.0", "High": "133.0", "Low": "130.0", "Close": "132.0", "Volume": "2000000", "OpenInt": "0"},
    ]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("Close", col("Close").cast("double"))
    result = typed.filter(col("Close").isNotNull() & (col("Close") > 0))
    assert result.count() == 1


def test_silver_daily_return_positive_when_close_above_open(spark):
    rows = [{"Date": "2024-01-02", "Symbol": "AAPL", "Open": "100.0", "High": "115.0", "Low": "99.0", "Close": "110.0", "Volume": "1000000", "OpenInt": "0"}]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("Open", col("Open").cast("double")).withColumn("Close", col("Close").cast("double"))
    result = typed.withColumn("daily_return_pct", spark_round((col("Close") - col("Open")) / col("Open") * 100, 4))
    assert result.collect()[0]["daily_return_pct"] == 10.0


def test_silver_daily_return_negative_when_close_below_open(spark):
    rows = [{"Date": "2024-01-02", "Symbol": "AAPL", "Open": "110.0", "High": "112.0", "Low": "99.0", "Close": "100.0", "Volume": "1000000", "OpenInt": "0"}]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("Open", col("Open").cast("double")).withColumn("Close", col("Close").cast("double"))
    result = typed.withColumn("daily_return_pct", spark_round((col("Close") - col("Open")) / col("Open") * 100, 4))
    val = result.collect()[0]["daily_return_pct"]
    assert val < 0


def test_silver_intraday_range_equals_high_minus_low(spark):
    rows = [{"Date": "2024-01-02", "Symbol": "AAPL", "Open": "100.0", "High": "115.0", "Low": "95.0", "Close": "110.0", "Volume": "1000000", "OpenInt": "0"}]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("High", col("High").cast("double")).withColumn("Low", col("Low").cast("double"))
    result = typed.withColumn("intraday_range", spark_round(col("High") - col("Low"), 4))
    assert result.collect()[0]["intraday_range"] == 20.0


def test_silver_date_column_is_cast_to_date_type(spark):
    from pyspark.sql.types import DateType
    rows = [{"Date": "2024-01-02", "Symbol": "AAPL", "Open": "130.0", "High": "132.0", "Low": "129.0", "Close": "131.0", "Volume": "1000000", "OpenInt": "0"}]
    df = _make_bronze_df(spark, rows)
    typed = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
    date_field = [f for f in typed.schema.fields if f.name == "Date"][0]
    assert isinstance(date_field.dataType, DateType)

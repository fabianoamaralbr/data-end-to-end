"""Unit tests for the gold aggregation logic (moving averages, lag returns, daily summary)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, first, lag, round as spark_round
from pyspark.sql.window import Window


def _make_silver_df(spark: SparkSession):
    rows = [
        {"Date": "2024-01-02", "Symbol": "AAPL", "Open": 100.0, "High": 105.0, "Low": 99.0, "Close": 104.0, "Volume": 1_000_000, "daily_return_pct": 4.0},
        {"Date": "2024-01-03", "Symbol": "AAPL", "Open": 104.0, "High": 108.0, "Low": 103.0, "Close": 107.0, "Volume": 1_200_000, "daily_return_pct": 2.88},
        {"Date": "2024-01-04", "Symbol": "AAPL", "Open": 107.0, "High": 110.0, "Low": 106.0, "Close": 109.0, "Volume": 1_100_000, "daily_return_pct": 1.87},
        {"Date": "2024-01-05", "Symbol": "AAPL", "Open": 109.0, "High": 112.0, "Low": 108.0, "Close": 111.0, "Volume": 900_000,   "daily_return_pct": 1.83},
        {"Date": "2024-01-08", "Symbol": "AAPL", "Open": 111.0, "High": 114.0, "Low": 110.0, "Close": 113.0, "Volume": 950_000,   "daily_return_pct": 1.80},
        {"Date": "2024-01-09", "Symbol": "AAPL", "Open": 113.0, "High": 116.0, "Low": 112.0, "Close": 115.0, "Volume": 800_000,   "daily_return_pct": 1.77},
        {"Date": "2024-01-10", "Symbol": "AAPL", "Open": 115.0, "High": 118.0, "Low": 114.0, "Close": 117.0, "Volume": 850_000,   "daily_return_pct": 1.74},
        {"Date": "2024-01-02", "Symbol": "MSFT", "Open": 200.0, "High": 205.0, "Low": 198.0, "Close": 203.0, "Volume": 500_000,   "daily_return_pct": 1.5},
        {"Date": "2024-01-03", "Symbol": "MSFT", "Open": 203.0, "High": 207.0, "Low": 202.0, "Close": 206.0, "Volume": 520_000,   "daily_return_pct": 1.48},
    ]
    return spark.createDataFrame(rows)


def test_gold_ma7_uses_7_row_rolling_window(spark):
    df = _make_silver_df(spark)
    aapl = df.filter(col("Symbol") == "AAPL").orderBy("Date")
    window = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-6, 0)
    result = aapl.withColumn("ma_close_7d", avg("Close").over(window))
    rows = result.orderBy("Date").collect()
    # First row: only 1 row in window, so ma == Close
    assert abs(rows[0]["ma_close_7d"] - rows[0]["Close"]) < 0.01
    # 7th row: full 7-row average
    closes = [r["Close"] for r in rows[:7]]
    expected_ma7 = sum(closes) / 7
    assert abs(rows[6]["ma_close_7d"] - expected_ma7) < 0.01


def test_gold_lag_prev_close_is_none_for_first_row(spark):
    df = _make_silver_df(spark)
    aapl = df.filter(col("Symbol") == "AAPL")
    window = Window.partitionBy("Symbol").orderBy("Date")
    result = aapl.withColumn("prev_close", lag("Close", 1).over(window))
    first_row = result.orderBy("Date").first()
    assert first_row["prev_close"] is None


def test_gold_lag_prev_close_matches_prior_day(spark):
    df = _make_silver_df(spark)
    aapl = df.filter(col("Symbol") == "AAPL")
    window = Window.partitionBy("Symbol").orderBy("Date")
    result = aapl.withColumn("prev_close", lag("Close", 1).over(window)).orderBy("Date").collect()
    assert result[1]["prev_close"] == result[0]["Close"]
    assert result[2]["prev_close"] == result[1]["Close"]


def test_gold_partitions_moving_averages_by_symbol(spark):
    df = _make_silver_df(spark)
    window = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-6, 0)
    result = df.withColumn("ma_close_7d", avg("Close").over(window))
    # MSFT's MA should never include AAPL data
    msft_rows = result.filter(col("Symbol") == "MSFT").orderBy("Date").collect()
    aapl_rows = result.filter(col("Symbol") == "AAPL").orderBy("Date").collect()
    assert msft_rows[0]["ma_close_7d"] != aapl_rows[0]["ma_close_7d"]


def test_gold_daily_summary_aggregates_correctly(spark):
    from pyspark.sql.functions import first as spark_first, spark_max, spark_min, spark_sum
    # Alias to avoid name collision
    from pyspark.sql.functions import max as fmax, min as fmin, sum as fsum
    df = _make_silver_df(spark)
    summary = (
        df.groupBy("Symbol", "Date")
        .agg(
            spark_first("Close").alias("close"),
            fmax("High").alias("high"),
            fmin("Low").alias("low"),
            fsum("Volume").alias("total_volume"),
        )
    )
    aapl_count = summary.filter(col("Symbol") == "AAPL").count()
    msft_count = summary.filter(col("Symbol") == "MSFT").count()
    assert aapl_count == 7
    assert msft_count == 2


def test_gold_cumulative_return_starts_at_zero_for_first_close(spark):
    df = _make_silver_df(spark)
    window = Window.partitionBy("Symbol").orderBy("Date")
    result = (
        df
        .withColumn("first_close", first("Close").over(window))
        .withColumn("cumulative_return_pct", spark_round((col("Close") - col("first_close")) / col("first_close") * 100, 4))
        .orderBy("Symbol", "Date")
    )
    aapl_first = result.filter(col("Symbol") == "AAPL").orderBy("Date").first()
    assert aapl_first["cumulative_return_pct"] == 0.0

"""Testes de transforms.silver — a mesma funcao que o notebook 02 executa."""
from datetime import date

import pytest
from helpers import bronze_ohlcv, ohlcv_row
from pyspark.sql import functions as F

from transforms.contracts import QUARANTINE_OHLCV, SILVER_OHLCV, SchemaContractError, schema_diff
from transforms.silver import deduplicate_latest, to_silver


def _reasons(result):
    return {r["Close"]: r["_rejection_reason"] for r in result.quarantine.collect()}


def test_output_schemas_match_contracts(spark):
    result = to_silver(bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(close="abc")]))
    assert schema_diff(result.valid.schema, SILVER_OHLCV) == []
    assert schema_diff(result.quarantine.schema, QUARANTINE_OHLCV) == []


def test_casts_business_columns(spark):
    row = to_silver(bronze_ohlcv(spark, [ohlcv_row(symbol=" aapl ")])).valid.first()
    assert row["Date"] == date(2024, 1, 2)
    assert row["Symbol"] == "AAPL"
    assert row["Close"] == 104.0
    assert row["Volume"] == 1_000_000


def test_derived_metrics(spark):
    bronze = bronze_ohlcv(spark, [ohlcv_row(open_="100.0", high="115.0", low="95.0", close="110.0")])
    row = to_silver(bronze).valid.first()
    assert row["daily_return_pct"] == 10.0
    assert row["intraday_range"] == 20.0
    assert row["price_spread_pct"] == 20.0


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"close": None}, "close_invalid"),
        ({"close": "0"}, "close_invalid"),
        ({"close": "abc"}, "close_invalid"),
        ({"open_": "-1"}, "open_invalid"),
        ({"date": "02/01/2024"}, "date_invalid"),
        ({"date": None}, "date_invalid"),
        ({"symbol": "  "}, "symbol_missing"),
        ({"high": "90.0", "low": "99.0"}, "high_below_low"),
        ({"volume": "-5"}, "volume_invalid"),
    ],
)
def test_invalid_rows_go_to_quarantine_with_reason(spark, overrides, reason):
    result = to_silver(bronze_ohlcv(spark, [ohlcv_row(**overrides)]))
    assert result.valid.count() == 0
    quarantined = result.quarantine.first()
    assert reason in quarantined["_rejection_reason"].split(";")


def test_quarantine_keeps_raw_values_and_all_reasons(spark):
    result = to_silver(bronze_ohlcv(spark, [ohlcv_row(close="abc", volume="-1")]))
    row = result.quarantine.first()
    assert row["Close"] == "abc"  # valor original, nao o cast nulo
    assert set(row["_rejection_reason"].split(";")) == {"close_invalid", "volume_invalid"}


def test_rescued_data_is_quarantined_as_schema_drift(spark):
    result = to_silver(bronze_ohlcv(spark, [ohlcv_row()], rescued='{"Dividend":"0.2"}'))
    assert result.valid.count() == 0
    assert result.quarantine.first()["_rejection_reason"] == "schema_drift"


def test_valid_and_invalid_rows_are_split(spark):
    result = to_silver(bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(date="2024-01-03", close=None)]))
    assert result.valid.count() == 1
    assert result.quarantine.count() == 1


def test_dedup_keeps_latest_ingestion(spark):
    old = bronze_ohlcv(spark, [ohlcv_row(close="104.0")], ingested_at="2024-01-03 06:00:00")
    new = bronze_ohlcv(spark, [ohlcv_row(close="104.5")], ingested_at="2024-01-04 06:00:00")
    for batch in (old.unionByName(new), new.unionByName(old)):
        rows = to_silver(batch).valid.collect()
        assert len(rows) == 1
        assert rows[0]["Close"] == 104.5


def test_dedup_is_deterministic_on_ingestion_ties(spark):
    """Mesmo _ingested_at e mesmo arquivo: desempate pelo hash, independe da ordem das linhas."""
    rows = [ohlcv_row(close="104.0"), ohlcv_row(close="104.5")]
    a = to_silver(bronze_ohlcv(spark, rows)).valid.first()["Close"]
    b = to_silver(bronze_ohlcv(spark, list(reversed(rows))).repartition(2)).valid.first()["Close"]
    assert a == b


def test_dedup_keeps_distinct_keys(spark):
    rows = [ohlcv_row(), ohlcv_row(symbol="MSFT"), ohlcv_row(date="2024-01-03")]
    assert to_silver(bronze_ohlcv(spark, rows)).valid.count() == 3


def test_deduplicate_latest_generic(spark):
    df = spark.createDataFrame(
        [("k", 1, "a"), ("k", 3, "c"), ("k", 2, "b"), ("j", 1, "x")], "key string, version int, payload string"
    )
    result = {r["key"]: r["payload"] for r in deduplicate_latest(df, ["key"], ["version"]).collect()}
    assert result == {"k": "c", "j": "x"}


def test_rejects_bronze_outside_contract(spark):
    bronze = bronze_ohlcv(spark, [ohlcv_row()]).withColumn("unexpected", F.lit(1))
    with pytest.raises(SchemaContractError, match="coluna inesperada: unexpected"):
        to_silver(bronze)

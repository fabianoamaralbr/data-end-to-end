"""Testes de transforms.bronze — chave de ingestao e metadados."""
from helpers import autoloader_batch, ohlcv_row

from transforms.bronze import add_ingestion_metadata
from transforms.contracts import BRONZE_OHLCV, OHLCV_BUSINESS_COLUMNS, conform


def _keys(spark, rows, **kwargs):
    batch = autoloader_batch(spark, rows, **kwargs)
    bronze = add_ingestion_metadata(batch, business_columns=OHLCV_BUSINESS_COLUMNS, run_id="r1")
    return [r["_ingestion_key"] for r in bronze.collect()]


def test_adds_metadata_and_matches_contract(spark):
    batch = autoloader_batch(spark, [ohlcv_row()])
    bronze = add_ingestion_metadata(batch, business_columns=OHLCV_BUSINESS_COLUMNS, run_id="r1")
    row = conform(bronze, BRONZE_OHLCV, context="test").first()
    assert row["_run_id"] == "r1"
    assert row["_ingested_at"] is not None
    assert len(row["_ingestion_key"]) == 64


def test_ingestion_key_is_deterministic_across_runs(spark):
    assert _keys(spark, [ohlcv_row()]) == _keys(spark, [ohlcv_row()])


def test_ingestion_key_changes_with_content(spark):
    assert _keys(spark, [ohlcv_row(close="1.0")]) != _keys(spark, [ohlcv_row(close="2.0")])


def test_ingestion_key_changes_with_source_file(spark):
    a = _keys(spark, [ohlcv_row()], source_file="abfss://raw@st/financial-data/a.csv")
    b = _keys(spark, [ohlcv_row()], source_file="abfss://raw@st/financial-data/b.csv")
    assert a != b


def test_ingestion_key_distinguishes_null_from_empty(spark):
    assert _keys(spark, [ohlcv_row(close=None)]) != _keys(spark, [ohlcv_row(close="")])


def test_ingestion_key_has_no_concatenation_collisions(spark):
    a = _keys(spark, [ohlcv_row(open_="1", high="23")])
    b = _keys(spark, [ohlcv_row(open_="12", high="3")])
    assert a != b


def test_ingestion_key_includes_rescued_data(spark):
    assert _keys(spark, [ohlcv_row()]) != _keys(spark, [ohlcv_row()], rescued='{"x":"1"}')

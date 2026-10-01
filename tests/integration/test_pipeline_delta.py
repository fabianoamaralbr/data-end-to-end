"""Testes com tabelas Delta reais: idempotencia, incrementalidade, quarentena e contratos.

Usam as mesmas funcoes ``process_*_batch`` que os notebooks passam ao ``foreachBatch``.
"""
import uuid

import pytest
from delta.tables import DeltaTable
from helpers import autoloader_batch, bronze_ohlcv, ohlcv_row
from pyspark.sql import functions as F

from transforms.batch import process_bronze_batch, process_gold_batch, process_silver_batch
from transforms.contracts import SchemaContractError
from transforms.datasets import DQ_METRICS, GOLD_OHLCV_DAILY_SUMMARY, GOLD_OHLCV_DETAIL, OHLCV
from transforms.quality import DataQualityError
from transforms.tables import ensure_table, optimize

pytestmark = pytest.mark.delta


@pytest.fixture
def tables(spark):
    """Cria todas as tabelas OHLCV num database isolado, a partir dos contratos."""
    db = f"t_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db}")
    specs = {
        "bronze": OHLCV.bronze, "silver": OHLCV.silver, "quarantine": OHLCV.quarantine,
        "metrics": DQ_METRICS, "detail": GOLD_OHLCV_DETAIL, "summary": GOLD_OHLCV_DAILY_SUMMARY,
    }
    names = {}
    for key, spec in specs.items():
        names[key] = f"{db}.{spec.name.replace('.', '_')}"
        ensure_table(spark, names[key], spec.contract, properties=spec.properties)
    yield names
    spark.sql(f"DROP DATABASE {db} CASCADE")


def _dt(spark, name):
    return DeltaTable.forName(spark, name)


def _silver(spark, tables, batch_df, batch_id=0, run_id="run-1", max_rate=0.5):
    return process_silver_batch(
        batch_df, batch_id, dataset=OHLCV,
        silver=_dt(spark, tables["silver"]), quarantine=_dt(spark, tables["quarantine"]),
        metrics=_dt(spark, tables["metrics"]), run_id=run_id, max_quarantine_rate=max_rate,
    )


# ── Bronze ────────────────────────────────────────────────────────────────────
def test_bronze_rerun_of_same_file_does_not_duplicate(spark, tables):
    batch = autoloader_batch(spark, [ohlcv_row(), ohlcv_row(symbol="MSFT")])
    for run in ("run-1", "run-2"):
        process_bronze_batch(batch, 0, dataset=OHLCV, target=_dt(spark, tables["bronze"]), run_id=run)
    bronze = spark.table(tables["bronze"])
    assert bronze.count() == 2
    assert bronze.select("_run_id").distinct().collect()[0]["_run_id"] == "run-1"


def test_bronze_appends_new_versions_of_a_record(spark, tables):
    target = _dt(spark, tables["bronze"])
    for batch_id, close in enumerate(["104.0", "104.5"]):
        batch = autoloader_batch(spark, [ohlcv_row(close=close)])
        process_bronze_batch(batch, batch_id, dataset=OHLCV, target=target, run_id="r")
    assert spark.table(tables["bronze"]).count() == 2


# ── Silver ────────────────────────────────────────────────────────────────────
def test_silver_replay_is_idempotent(spark, tables):
    batch = bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(symbol="MSFT"), ohlcv_row(date="2024-01-03", close="0")])
    _silver(spark, tables, batch)
    before = spark.table(tables["silver"]).drop("_transformed_at").collect()
    _silver(spark, tables, batch)
    assert sorted(spark.table(tables["silver"]).drop("_transformed_at").collect()) == sorted(before)
    assert spark.table(tables["quarantine"]).count() == 1


def test_silver_is_incremental_and_keeps_latest_version(spark, tables):
    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row(close="104.0"), ohlcv_row(symbol="MSFT")],
                                        ingested_at="2024-01-03 06:00:00"), batch_id=0)
    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row(close="104.5"), ohlcv_row(date="2024-01-03")],
                                        ingested_at="2024-01-04 06:00:00"), batch_id=1)
    rows = {(r["Symbol"], str(r["Date"])): r["Close"] for r in spark.table(tables["silver"]).collect()}
    assert rows == {("AAPL", "2024-01-02"): 104.5, ("MSFT", "2024-01-02"): 104.0, ("AAPL", "2024-01-03"): 104.0}


def test_silver_ignores_late_arriving_older_version(spark, tables):
    newer = bronze_ohlcv(spark, [ohlcv_row(close="104.5")], ingested_at="2024-01-04 06:00:00")
    older = bronze_ohlcv(spark, [ohlcv_row(close="104.0")], ingested_at="2024-01-03 06:00:00")
    _silver(spark, tables, newer, batch_id=0)
    _silver(spark, tables, older, batch_id=1)
    assert spark.table(tables["silver"]).first()["Close"] == 104.5


def test_silver_records_quality_metrics(spark, tables):
    batch = bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(), ohlcv_row(close="104.9"), ohlcv_row(date="x")])
    metrics = _silver(spark, tables, batch, batch_id=7)
    row = spark.table(tables["metrics"]).filter(F.col("batch_id") == 7).first()
    assert (row["rows_in"], row["rows_valid"], row["rows_quarantined"], row["rows_deduplicated"]) == (4, 1, 1, 2)
    assert row["quarantine_rate"] == metrics.quarantine_rate == 0.25


def test_circuit_breaker_blocks_silver_but_keeps_evidence(spark, tables):
    batch = bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(date="2024-01-03", close=None)])
    with pytest.raises(DataQualityError):
        _silver(spark, tables, batch, max_rate=0.1)
    assert spark.table(tables["silver"]).count() == 0
    assert spark.table(tables["quarantine"]).count() == 1
    assert spark.table(tables["metrics"]).count() == 1


# ── Gold ──────────────────────────────────────────────────────────────────────
def _gold(spark, tables, changes):
    return process_gold_batch(
        changes, 0, silver_table=tables["silver"],
        detail=_dt(spark, tables["detail"]), summary=_dt(spark, tables["summary"]),
    )


def _changes(spark, symbols):
    return spark.createDataFrame([(s, "insert") for s in symbols], "Symbol string, _change_type string")


def test_gold_recomputes_only_changed_symbols(spark, tables):
    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(symbol="MSFT")]))
    assert _gold(spark, tables, _changes(spark, ["AAPL", "MSFT"])) == ["AAPL", "MSFT"]
    msft_before = spark.table(tables["detail"]).filter("Symbol = 'MSFT'").first()["_gold_created_at"]

    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row(date="2024-01-03", close="110.0")],
                                        ingested_at="2024-01-04 06:00:00"), batch_id=1)
    assert _gold(spark, tables, _changes(spark, ["AAPL"])) == ["AAPL"]

    detail = spark.table(tables["detail"])
    assert detail.filter("Symbol = 'MSFT'").first()["_gold_created_at"] == msft_before
    aapl = detail.filter("Symbol = 'AAPL'").orderBy("Date").collect()
    assert [r["prev_close"] for r in aapl] == [None, 104.0]
    assert spark.table(tables["summary"]).count() == 3


def test_gold_replay_is_idempotent_and_ignores_deletes(spark, tables):
    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row()]))
    changes = spark.createDataFrame(
        [("AAPL", "insert"), ("AAPL", "insert"), ("MSFT", "update_preimage")], "Symbol string, _change_type string"
    )
    assert _gold(spark, tables, changes) == ["AAPL"]
    assert _gold(spark, tables, changes) == ["AAPL"]
    assert spark.table(tables["detail"]).count() == 1
    assert _gold(spark, tables, _changes(spark, [])) == []


# ── Contratos de tabela ───────────────────────────────────────────────────────
def test_ensure_table_fails_on_schema_drift(spark, tables):
    spark.sql(f"ALTER TABLE {tables['silver']} ADD COLUMNS (surprise STRING)")
    with pytest.raises(SchemaContractError, match="coluna inesperada: surprise"):
        ensure_table(spark, tables["silver"], OHLCV.silver.contract)


def test_delta_rejects_write_outside_contract(spark, tables):
    """Sem mergeSchema/overwriteSchema, o proprio Delta recusa colunas novas."""
    df = spark.table(tables["summary"]).withColumn("surprise", F.lit(1))
    with pytest.raises(Exception, match="(?i)schema"):
        df.write.format("delta").mode("append").saveAsTable(tables["summary"])


def test_optimize_zorder_runs_on_contract_tables(spark, tables):
    _silver(spark, tables, bronze_ohlcv(spark, [ohlcv_row(), ohlcv_row(symbol="MSFT")]))
    optimize(spark, tables["silver"], OHLCV.silver.zorder_by)
    assert spark.table(tables["silver"]).count() == 2

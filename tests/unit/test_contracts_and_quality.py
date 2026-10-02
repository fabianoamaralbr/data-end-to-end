"""Testes de contratos, DDL, regras de qualidade e condicoes de MERGE (sem escrita Delta)."""
import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from transforms.contracts import DQ_METRICS, SILVER_OHLCV, SchemaContractError, schema_diff, validate_contract
from transforms.datasets import ALL_TABLES, get_dataset
from transforms.delta_ops import key_condition, newer_than_condition
from transforms.quality import BatchMetrics, DataQualityError, check_quarantine_rate
from transforms.tables import create_table_ddl

EXPECTED = StructType([StructField("a", StringType()), StructField("b", LongType())])


def test_schema_diff_reports_missing_extra_and_type_changes():
    actual = StructType([StructField("b", DoubleType()), StructField("c", StringType())])
    assert schema_diff(actual, EXPECTED) == [
        "coluna ausente: a",
        "coluna inesperada: c",
        "tipo divergente em b: esperado bigint, recebido double",
    ]


def test_schema_diff_ignores_order_and_nullability():
    actual = StructType([StructField("b", LongType(), False), StructField("a", StringType())])
    assert schema_diff(actual, EXPECTED) == []


def test_validate_contract_raises_with_context():
    with pytest.raises(SchemaContractError, match=r"\[silver.x\].*coluna ausente: b"):
        validate_contract(StructType([StructField("a", StringType())]), EXPECTED, context="silver.x")


def test_ddl_declares_types_not_null_and_properties():
    ddl = create_table_ddl("cat.silver.ohlcv", SILVER_OHLCV, location="abfss://silver@st/x",
                           properties={"delta.enableChangeDataFeed": "true"})
    assert "`Date` date NOT NULL" in ddl
    assert "`Volume` bigint," in ddl
    assert "LOCATION 'abfss://silver@st/x'" in ddl
    assert "'delta.enableDeletionVectors' = 'false'" in ddl
    assert "'delta.enableChangeDataFeed' = 'true'" in ddl
    assert "PARTITIONED" not in ddl.upper()


def test_every_table_has_unique_name_and_location():
    assert len({t.name for t in ALL_TABLES}) == len(ALL_TABLES)
    assert len({(t.container, t.path) for t in ALL_TABLES}) == len(ALL_TABLES)


def test_get_dataset_unknown():
    with pytest.raises(ValueError, match="dataset desconhecido"):
        get_dataset("nope")


def test_key_condition_is_null_safe():
    assert key_condition(["Symbol", "Date"]) == "t.`Symbol` <=> s.`Symbol` AND t.`Date` <=> s.`Date`"


def test_newer_than_condition_is_lexicographic():
    assert newer_than_condition(["a", "b"]) == "(s.`a` > t.`a`) OR (s.`a` = t.`a` AND s.`b` > t.`b`)"


def test_batch_metrics_derivations(spark):
    m = BatchMetrics("silver.ohlcv", 3, "run", rows_in=10, rows_valid=6, rows_quarantined=2)
    assert m.rows_deduplicated == 2
    assert m.quarantine_rate == 0.2
    row = m.to_df(spark).first()
    assert m.to_df(spark).schema == DQ_METRICS
    assert (row["rows_in"], row["rows_quarantined"], row["quarantine_rate"]) == (10, 2, 0.2)


def test_empty_batch_has_zero_rate():
    assert BatchMetrics("t", 0, "r", 0, 0, 0).quarantine_rate == 0.0


def test_circuit_breaker():
    check_quarantine_rate(BatchMetrics("t", 0, "r", 100, 95, 5), max_rate=0.05)
    with pytest.raises(DataQualityError, match="6/100"):
        check_quarantine_rate(BatchMetrics("t", 0, "r", 100, 94, 6), max_rate=0.05)


def test_table_spec_paths():
    from transforms.datasets import OHLCV

    assert OHLCV.silver.full_name("financial") == "financial.silver.ohlcv"
    assert OHLCV.silver.location("stacct") == "abfss://silver@stacct.dfs.core.windows.net/financial_data"
    assert OHLCV.checkpoint("stacct", "bronze") == "abfss://bronze@stacct.dfs.core.windows.net/_checkpoints/ohlcv"

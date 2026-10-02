"""Testes de transforms.sgs — series do Banco Central."""
from datetime import date
from decimal import Decimal

from helpers import bronze_sgs

from transforms.contracts import QUARANTINE_SGS, SILVER_SGS, schema_diff
from transforms.sgs import to_silver


def test_parses_series_from_path_and_types(spark):
    result = to_silver(bronze_sgs(spark, [{"data": "02/01/2024", "valor": "11.75"}], series_code=432))
    row = result.valid.first()
    assert schema_diff(result.valid.schema, SILVER_SGS) == []
    assert (row["series_code"], row["series_name"]) == (432, "selic_meta")
    assert row["ref_date"] == date(2024, 1, 2)
    assert row["value"] == Decimal("11.75")


def test_overlapping_windows_keep_latest_observation(spark):
    """Janelas moveis se sobrepoem; a observacao mais recente de cada data vence."""
    first = bronze_sgs(spark, [{"data": "02/01/2024", "valor": "4.9"}],
                       series_code=1, window="2024-01-02", ingested_at="2024-01-02 06:00:00")
    revised = bronze_sgs(spark, [{"data": "02/01/2024", "valor": "4.95"}, {"data": "03/01/2024", "valor": "4.97"}],
                         series_code=1, window="2024-01-03", ingested_at="2024-01-03 06:00:00")
    rows = {r["ref_date"]: r["value"] for r in to_silver(first.unionByName(revised)).valid.collect()}
    assert rows == {date(2024, 1, 2): Decimal("4.95"), date(2024, 1, 3): Decimal("4.97")}


def test_invalid_rows_quarantined(spark):
    rows = [{"data": "2024-01-02", "valor": "1"}, {"data": "03/01/2024", "valor": "n/a"}]
    result = to_silver(bronze_sgs(spark, rows))
    assert result.valid.count() == 0
    assert schema_diff(result.quarantine.schema, QUARANTINE_SGS) == []
    reasons = {r["data"]: r["_rejection_reason"] for r in result.quarantine.collect()}
    assert reasons == {"2024-01-02": "date_invalid", "03/01/2024": "value_invalid"}


def test_unknown_series_quarantined(spark):
    result = to_silver(bronze_sgs(spark, [{"data": "02/01/2024", "valor": "1"}], series_code=99999))
    assert result.quarantine.first()["_rejection_reason"] == "series_unknown"

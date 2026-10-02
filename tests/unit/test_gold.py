"""Testes de transforms.gold — a mesma funcao que o notebook 03 executa."""
import pytest
from helpers import bronze_ohlcv, ohlcv_row

from transforms.contracts import GOLD_OHLCV_DAILY_SUMMARY, GOLD_OHLCV_DETAIL, schema_diff
from transforms.gold import build_daily_summary, build_detail
from transforms.silver import to_silver

AAPL_CLOSES = [104.0, 107.0, 109.0, 111.0, 113.0, 115.0, 117.0, 119.0]
DATES = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
         "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11"]


@pytest.fixture(scope="module")
def silver(spark):
    rows = [
        ohlcv_row(date=d, symbol="AAPL", open_="100.0", high="200.0", low="50.0",
                  close=str(c), volume=str(1000 * (i + 1)))
        for i, (d, c) in enumerate(zip(DATES, AAPL_CLOSES, strict=True))
    ]
    rows += [
        ohlcv_row(date="2024-01-02", symbol="MSFT", open_="200.0", high="210.0", low="190.0", close="203.0"),
        ohlcv_row(date="2024-01-03", symbol="MSFT", open_="203.0", high="210.0", low="190.0", close="206.0"),
    ]
    return to_silver(bronze_ohlcv(spark, rows)).valid


@pytest.fixture(scope="module")
def detail(silver):
    return build_detail(silver)


def _symbol(df, symbol):
    return df.filter(df.Symbol == symbol).orderBy("Date").collect()


def test_detail_and_summary_match_contracts(detail):
    assert schema_diff(detail.schema, GOLD_OHLCV_DETAIL) == []
    assert schema_diff(build_daily_summary(detail).schema, GOLD_OHLCV_DAILY_SUMMARY) == []


def test_ma7_rolling_window(detail):
    rows = _symbol(detail, "AAPL")
    # Janela incompleta: sem 7 pregoes ainda nao existe MA7
    assert all(r["ma_close_7d"] is None for r in rows[:6])
    assert rows[6]["ma_close_7d"] == pytest.approx(sum(AAPL_CLOSES[:7]) / 7, abs=1e-4)
    assert rows[7]["ma_close_7d"] == pytest.approx(sum(AAPL_CLOSES[1:8]) / 7, abs=1e-4)


def test_ma30_is_null_until_window_is_complete(detail):
    assert all(r["ma_close_30d"] is None for r in _symbol(detail, "AAPL"))


def test_ma_volume_7d(detail):
    rows = _symbol(detail, "AAPL")
    assert rows[5]["ma_volume_7d"] is None
    assert rows[6]["ma_volume_7d"] == 4000  # media de 1000..7000
    assert rows[7]["ma_volume_7d"] == 5000  # media de 2000..8000


def test_prev_close_and_day_over_day_return(detail):
    rows = _symbol(detail, "AAPL")
    assert rows[0]["prev_close"] is None
    assert rows[0]["day_over_day_return_pct"] is None
    assert rows[1]["prev_close"] == AAPL_CLOSES[0]
    assert rows[1]["day_over_day_return_pct"] == pytest.approx((107.0 - 104.0) / 104.0 * 100, abs=1e-4)


def test_cumulative_return(detail):
    rows = _symbol(detail, "AAPL")
    assert rows[0]["cumulative_return_pct"] == 0.0
    assert rows[-1]["cumulative_return_pct"] == pytest.approx((119.0 - 104.0) / 104.0 * 100, abs=1e-4)


def test_windows_are_partitioned_by_symbol(detail):
    msft = _symbol(detail, "MSFT")
    assert msft[0]["prev_close"] is None
    assert msft[0]["ma_close_7d"] is None  # nao herda pregoes do AAPL
    assert msft[1]["cumulative_return_pct"] == pytest.approx((206.0 - 203.0) / 203.0 * 100, abs=1e-4)


def test_daily_summary_is_one_row_per_symbol_date(detail):
    summary = build_daily_summary(detail)
    assert summary.count() == detail.count() == len(DATES) + 2
    assert summary.select("Symbol", "Date").distinct().count() == summary.count()
    row = _symbol(summary, "MSFT")[0]
    assert (row["open"], row["close"], row["total_volume"]) == (200.0, 203.0, 1_000_000)

"""Gera um CSV de amostra financeira realista com 500 linhas para desenvolvimento local e testes."""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

TICKERS = {
    "AAPL":  {"base": 130.0, "vol": 3.0},
    "MSFT":  {"base": 240.0, "vol": 4.0},
    "GOOGL": {"base": 2800.0, "vol": 35.0},
    "AMZN":  {"base": 3200.0, "vol": 45.0},
    "TSLA":  {"base": 700.0, "vol": 20.0},
}

START_DATE = date(2020, 1, 2)
ROWS_PER_TICKER = 100

random.seed(42)


def generate_ohlcv(base_price: float, vol: float):
    open_p = round(base_price + random.gauss(0, vol), 2)
    close_p = round(open_p + random.gauss(0, vol * 0.5), 2)
    high_p = round(max(open_p, close_p) + abs(random.gauss(0, vol * 0.3)), 2)
    low_p = round(min(open_p, close_p) - abs(random.gauss(0, vol * 0.3)), 2)
    volume = int(random.randint(5_000_000, 80_000_000))
    open_int = 0
    return open_p, high_p, low_p, close_p, volume, open_int


def main():
    out_path = Path(__file__).parent.parent / "data" / "sample" / "financial_sample.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for ticker, params in TICKERS.items():
        current_price = params["base"]
        current_date = START_DATE
        for _ in range(ROWS_PER_TICKER):
            # pula finais de semana
            while current_date.weekday() >= 5:
                current_date += timedelta(days=1)

            open_p, high_p, low_p, close_p, vol, oi = generate_ohlcv(current_price, params["vol"])
            rows.append({
                "Date": current_date.isoformat(),
                "Symbol": ticker,
                "Open": open_p,
                "High": high_p,
                "Low": low_p,
                "Close": close_p,
                "Volume": vol,
                "OpenInt": oi,
            })
            current_price = close_p
            current_date += timedelta(days=1)

    rows.sort(key=lambda r: (r["Date"], r["Symbol"]))

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Symbol", "Open", "High", "Low", "Close", "Volume", "OpenInt"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Geradas {len(rows)} linhas -> {out_path}")


if __name__ == "__main__":
    main()

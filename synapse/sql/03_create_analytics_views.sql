-- =============================================================================
-- 03 — Analytics Views — business-ready queries for Power BI and ad-hoc analysis
-- =============================================================================

USE financial_analytics;
GO

-- ── 1. Latest price per symbol ────────────────────────────────────────────────
CREATE OR ALTER VIEW gold.vw_latest_price AS
SELECT
    Symbol,
    MAX([Date])                                             AS latest_date,
    MAX(close) OVER (PARTITION BY Symbol ORDER BY [Date]
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latest_close,
    AVG(daily_return_pct)                                   AS avg_daily_return_pct,
    SUM(total_volume)                                       AS total_volume_all_time
FROM gold.daily_summary
GROUP BY Symbol, [Date], close, daily_return_pct, total_volume;
GO

-- ── 2. Rolling 30-day volatility (std dev of daily returns) ───────────────────
CREATE OR ALTER VIEW gold.vw_rolling_volatility AS
SELECT
    Symbol,
    [Date],
    close,
    daily_return_pct,
    ma_close_7d,
    ma_close_30d,
    day_over_day_return_pct,
    cumulative_return_pct,
    CASE
        WHEN ma_close_7d > ma_close_30d THEN 'BULLISH'
        WHEN ma_close_7d < ma_close_30d THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS trend_signal
FROM gold.daily_summary;
GO

-- ── 3. Symbol performance ranking (by cumulative return) ─────────────────────
CREATE OR ALTER VIEW gold.vw_symbol_performance AS
SELECT
    Symbol,
    MIN([Date])                 AS period_start,
    MAX([Date])                 AS period_end,
    FIRST_VALUE(close)          OVER (PARTITION BY Symbol ORDER BY [Date])   AS start_price,
    LAST_VALUE(close)           OVER (PARTITION BY Symbol ORDER BY [Date]
                                     ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND UNBOUNDED FOLLOWING)                 AS end_price,
    MAX(cumulative_return_pct)  AS max_cumulative_return_pct,
    AVG(daily_return_pct)       AS avg_daily_return_pct,
    MAX(high)                   AS all_time_high,
    MIN(low)                    AS all_time_low,
    SUM(total_volume)           AS total_volume
FROM gold.daily_summary
GROUP BY Symbol, [Date], close, cumulative_return_pct, daily_return_pct, high, low, total_volume;
GO

-- ── 4. Data quality summary (bronze vs silver reconciliation) ─────────────────
CREATE OR ALTER VIEW gold.vw_data_quality_check AS
SELECT
    'bronze' AS layer,
    COUNT(*)  AS total_rows,
    COUNT(DISTINCT Symbol) AS distinct_symbols,
    MIN(TRY_CAST([Date] AS DATE)) AS min_date,
    MAX(TRY_CAST([Date] AS DATE)) AS max_date
FROM bronze.financial_raw
UNION ALL
SELECT
    'silver'  AS layer,
    COUNT(*)  AS total_rows,
    COUNT(DISTINCT Symbol) AS distinct_symbols,
    MIN([Date]) AS min_date,
    MAX([Date]) AS max_date
FROM silver.financial_clean
UNION ALL
SELECT
    'gold'    AS layer,
    COUNT(*)  AS total_rows,
    COUNT(DISTINCT Symbol) AS distinct_symbols,
    MIN([Date]) AS min_date,
    MAX([Date]) AS max_date
FROM gold.daily_summary;
GO

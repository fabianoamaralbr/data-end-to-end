-- =============================================================================
-- 03 — Analytics Views — business-ready queries for Power BI and ad-hoc analysis
-- =============================================================================

USE financial_analytics;
GO

-- ── 1. Latest price per symbol ────────────────────────────────────────────────
-- One row per symbol: the most recent trading day and its closing price.
CREATE OR ALTER VIEW gold.vw_latest_price AS
WITH ranked AS (
    SELECT
        Symbol,
        [Date],
        close,
        ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY [Date] DESC) AS rn
    FROM gold.daily_summary
),
totals AS (
    SELECT
        Symbol,
        AVG(intraday_return_pct) AS avg_intraday_return_pct,
        SUM(total_volume)        AS total_volume_all_time
    FROM gold.daily_summary
    GROUP BY Symbol
)
SELECT
    r.Symbol,
    r.[Date]                    AS latest_date,
    r.close                     AS latest_close,
    t.avg_intraday_return_pct,
    t.total_volume_all_time
FROM ranked AS r
JOIN totals AS t ON t.Symbol = r.Symbol
WHERE r.rn = 1;
GO

-- ── 2. Rolling 30-day volatility + trend signal ───────────────────────────────
-- volatility_30d_pct: sample std dev of the last 30 day-over-day returns (percentage
-- points, not annualized). NULL until 30 returns exist, consistent with ma_close_30d.
CREATE OR ALTER VIEW gold.vw_rolling_volatility AS
WITH returns AS (
    SELECT
        Symbol,
        [Date],
        close,
        intraday_return_pct,
        day_over_day_return_pct,
        cumulative_return_pct,
        ma_close_7d,
        ma_close_30d,
        STDEV(day_over_day_return_pct) OVER (PARTITION BY Symbol ORDER BY [Date] ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS stdev_30,
        COUNT(day_over_day_return_pct) OVER (PARTITION BY Symbol ORDER BY [Date] ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS n_returns_30
    FROM gold.daily_summary
)
SELECT
    Symbol,
    [Date],
    close,
    intraday_return_pct,
    day_over_day_return_pct,
    cumulative_return_pct,
    ma_close_7d,
    ma_close_30d,
    CASE WHEN n_returns_30 = 30 THEN stdev_30 END AS volatility_30d_pct,
    CASE
        WHEN ma_close_7d IS NULL OR ma_close_30d IS NULL THEN NULL
        WHEN ma_close_7d > ma_close_30d THEN 'BULLISH'
        WHEN ma_close_7d < ma_close_30d THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS trend_signal
FROM returns;
GO

-- ── 3. Symbol performance ranking (by total return over the period) ──────────
CREATE OR ALTER VIEW gold.vw_symbol_performance AS
WITH bounds AS (
    SELECT
        Symbol,
        MIN([Date])                  AS period_start,
        MAX([Date])                  AS period_end,
        MAX(high)                    AS period_high,
        MIN(low)                     AS period_low,
        AVG(intraday_return_pct)     AS avg_intraday_return_pct,
        STDEV(day_over_day_return_pct) AS stdev_daily_return_pct,
        SUM(total_volume)            AS total_volume,
        COUNT(*)                     AS trading_days
    FROM gold.daily_summary
    GROUP BY Symbol
)
SELECT
    b.Symbol,
    b.period_start,
    b.period_end,
    s.close                                         AS start_price,
    e.close                                         AS end_price,
    (e.close - s.close) / NULLIF(s.close, 0) * 100  AS total_return_pct,
    b.period_high,
    b.period_low,
    b.avg_intraday_return_pct,
    b.stdev_daily_return_pct,
    b.total_volume,
    b.trading_days,
    RANK() OVER (ORDER BY (e.close - s.close) / NULLIF(s.close, 0) DESC) AS return_rank
FROM bounds AS b
JOIN gold.daily_summary AS s ON s.Symbol = b.Symbol AND s.[Date] = b.period_start
JOIN gold.daily_summary AS e ON e.Symbol = b.Symbol AND e.[Date] = b.period_end;
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

-- ── 5. Data quality monitoring (daily rollup of per-batch metrics) ────────────
-- Source for alerting/dashboards: quarantine rate per table per day.
CREATE OR ALTER VIEW ops.vw_dq_daily AS
SELECT
    table_name,
    CAST(measured_at AS DATE)                                       AS measured_date,
    COUNT(*)                                                        AS batches,
    SUM(rows_in)                                                    AS rows_in,
    SUM(rows_valid)                                                 AS rows_valid,
    SUM(rows_quarantined)                                           AS rows_quarantined,
    SUM(rows_deduplicated)                                          AS rows_deduplicated,
    CAST(SUM(rows_quarantined) AS FLOAT) / NULLIF(SUM(rows_in), 0)  AS quarantine_rate
FROM ops.dq_metrics
GROUP BY table_name, CAST(measured_at AS DATE);
GO

-- ── 6. Quarantine breakdown by rejection reason ───────────────────────────────
CREATE OR ALTER VIEW ops.vw_quarantine_reasons AS
SELECT
    r.value                          AS rejection_reason,
    CAST(q._quarantined_at AS DATE)  AS quarantined_date,
    COUNT(*)                         AS rows_rejected
FROM silver.financial_quarantine AS q
CROSS APPLY STRING_SPLIT(q._rejection_reason, ';') AS r
GROUP BY r.value, CAST(q._quarantined_at AS DATE);
GO

-- =============================================================================
-- 02 — External Tables on Delta Lake layers
-- Serverless SQL reads Delta Lake transaction logs for schema + latest snapshot
-- =============================================================================

USE financial_analytics;
GO

-- ── Bronze ────────────────────────────────────────────────────────────────────
IF OBJECT_ID('bronze.financial_raw', 'ET') IS NOT NULL DROP EXTERNAL TABLE bronze.financial_raw;

CREATE EXTERNAL TABLE bronze.financial_raw
(
    [Date]              NVARCHAR(20),
    [Symbol]            NVARCHAR(10),
    [Open]              FLOAT,
    [High]              FLOAT,
    [Low]               FLOAT,
    [Close]             FLOAT,
    [Volume]            BIGINT,
    [OpenInt]           BIGINT,
    [_ingested_at]      NVARCHAR(50),
    [_source_file]      NVARCHAR(500),
    [_pipeline_run_ts]  NVARCHAR(50)
)
WITH (
    DATA_SOURCE     = eds_bronze,
    LOCATION        = 'financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Silver ────────────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.financial_clean', 'ET') IS NOT NULL DROP EXTERNAL TABLE silver.financial_clean;

CREATE EXTERNAL TABLE silver.financial_clean
(
    [Date]              DATE,
    [Symbol]            NVARCHAR(10),
    [Open]              FLOAT,
    [High]              FLOAT,
    [Low]               FLOAT,
    [Close]             FLOAT,
    [Volume]            BIGINT,
    [OpenInt]           BIGINT,
    [daily_return_pct]  FLOAT,
    [intraday_range]    FLOAT,
    [price_spread_pct]  FLOAT,
    [_ingested_at]      NVARCHAR(50),
    [_transformed_at]   NVARCHAR(50)
)
WITH (
    DATA_SOURCE     = eds_silver,
    LOCATION        = 'financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Gold — Detail with moving averages ────────────────────────────────────────
IF OBJECT_ID('gold.financial_detail', 'ET') IS NOT NULL DROP EXTERNAL TABLE gold.financial_detail;

CREATE EXTERNAL TABLE gold.financial_detail
(
    [Date]                      DATE,
    [Symbol]                    NVARCHAR(10),
    [Open]                      FLOAT,
    [High]                      FLOAT,
    [Low]                       FLOAT,
    [Close]                     FLOAT,
    [Volume]                    BIGINT,
    [daily_return_pct]          FLOAT,
    [intraday_range]            FLOAT,
    [price_spread_pct]          FLOAT,
    [ma_close_7d]               FLOAT,
    [ma_close_30d]              FLOAT,
    [ma_volume_7d]              BIGINT,
    [prev_close]                FLOAT,
    [day_over_day_return_pct]   FLOAT,
    [cumulative_return_pct]     FLOAT,
    [_gold_created_at]          NVARCHAR(50)
)
WITH (
    DATA_SOURCE     = eds_gold,
    LOCATION        = 'financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Gold — Daily Summary ──────────────────────────────────────────────────────
IF OBJECT_ID('gold.daily_summary', 'ET') IS NOT NULL DROP EXTERNAL TABLE gold.daily_summary;

CREATE EXTERNAL TABLE gold.daily_summary
(
    [Symbol]                    NVARCHAR(10),
    [Date]                      DATE,
    [open]                      FLOAT,
    [high]                      FLOAT,
    [low]                       FLOAT,
    [close]                     FLOAT,
    [total_volume]              BIGINT,
    [daily_return_pct]          FLOAT,
    [ma_close_7d]               FLOAT,
    [ma_close_30d]              FLOAT,
    [day_over_day_return_pct]   FLOAT,
    [cumulative_return_pct]     FLOAT,
    [row_count]                 INT
)
WITH (
    DATA_SOURCE     = eds_gold,
    LOCATION        = 'daily_summary',
    FILE_FORMAT     = ff_delta_parquet
);
GO

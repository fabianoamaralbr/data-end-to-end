-- =============================================================================
-- 02 — External Tables on Delta Lake layers
-- Serverless SQL reads Delta Lake transaction logs for schema + latest snapshot.
-- Column lists mirror the data contracts in src/transforms/contracts.py.
-- Delta tables are written with deletion vectors disabled (reader v1) so that
-- Serverless SQL can read them.
-- =============================================================================

USE financial_analytics;
GO

-- ── Bronze (raw strings + ingestion metadata) ─────────────────────────────────
IF OBJECT_ID('bronze.financial_raw', 'ET') IS NOT NULL DROP EXTERNAL TABLE bronze.financial_raw;

CREATE EXTERNAL TABLE bronze.financial_raw
(
    [Date]                NVARCHAR(50),
    [Symbol]              NVARCHAR(50),
    [Open]                NVARCHAR(50),
    [High]                NVARCHAR(50),
    [Low]                 NVARCHAR(50),
    [Close]               NVARCHAR(50),
    [Volume]              NVARCHAR(50),
    [OpenInt]             NVARCHAR(50),
    [_rescued_data]       NVARCHAR(4000),
    [_source_file]        NVARCHAR(1000),
    [_source_modified_at] DATETIME2(7),
    [_ingested_at]        DATETIME2(7),
    [_run_id]             NVARCHAR(100),
    [_ingestion_key]      CHAR(64)
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
    [Symbol]            NVARCHAR(50),
    [Open]              FLOAT,
    [High]              FLOAT,
    [Low]               FLOAT,
    [Close]             FLOAT,
    [Volume]            BIGINT,
    [OpenInt]           BIGINT,
    [daily_return_pct]  FLOAT,
    [intraday_range]    FLOAT,
    [price_spread_pct]  FLOAT,
    [_ingestion_key]    CHAR(64),
    [_source_file]      NVARCHAR(1000),
    [_ingested_at]      DATETIME2(7),
    [_transformed_at]   DATETIME2(7)
)
WITH (
    DATA_SOURCE     = eds_silver,
    LOCATION        = 'financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Silver — Quarantine (rejected rows: raw values + reason) ──────────────────
IF OBJECT_ID('silver.financial_quarantine', 'ET') IS NOT NULL DROP EXTERNAL TABLE silver.financial_quarantine;

CREATE EXTERNAL TABLE silver.financial_quarantine
(
    [Date]                NVARCHAR(50),
    [Symbol]              NVARCHAR(50),
    [Open]                NVARCHAR(50),
    [High]                NVARCHAR(50),
    [Low]                 NVARCHAR(50),
    [Close]               NVARCHAR(50),
    [Volume]              NVARCHAR(50),
    [OpenInt]             NVARCHAR(50),
    [_rescued_data]       NVARCHAR(4000),
    [_source_file]        NVARCHAR(1000),
    [_source_modified_at] DATETIME2(7),
    [_ingested_at]        DATETIME2(7),
    [_run_id]             NVARCHAR(100),
    [_ingestion_key]      CHAR(64),
    [_rejection_reason]   NVARCHAR(500),
    [_quarantined_at]     DATETIME2(7)
)
WITH (
    DATA_SOURCE     = eds_silver,
    LOCATION        = '_quarantine/financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Silver — Banco Central (SGS) ──────────────────────────────────────────────
IF OBJECT_ID('silver.bcb_sgs', 'ET') IS NOT NULL DROP EXTERNAL TABLE silver.bcb_sgs;

CREATE EXTERNAL TABLE silver.bcb_sgs
(
    [series_code]       INT,
    [series_name]       NVARCHAR(100),
    [ref_date]          DATE,
    [value]             DECIMAL(20, 8),
    [_ingestion_key]    CHAR(64),
    [_source_file]      NVARCHAR(1000),
    [_ingested_at]      DATETIME2(7),
    [_transformed_at]   DATETIME2(7)
)
WITH (
    DATA_SOURCE     = eds_silver,
    LOCATION        = 'bcb_sgs',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Gold — Detail with moving averages ────────────────────────────────────────
IF OBJECT_ID('gold.financial_detail', 'ET') IS NOT NULL DROP EXTERNAL TABLE gold.financial_detail;

CREATE EXTERNAL TABLE gold.financial_detail
(
    [Date]                      DATE,
    [Symbol]                    NVARCHAR(50),
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
    [_gold_created_at]          DATETIME2(7)
)
WITH (
    DATA_SOURCE     = eds_gold,
    LOCATION        = 'financial_data',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Gold — Daily Summary (one row per Symbol/Date) ────────────────────────────
IF OBJECT_ID('gold.daily_summary', 'ET') IS NOT NULL DROP EXTERNAL TABLE gold.daily_summary;

CREATE EXTERNAL TABLE gold.daily_summary
(
    [Symbol]                    NVARCHAR(50),
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
    [cumulative_return_pct]     FLOAT
)
WITH (
    DATA_SOURCE     = eds_gold,
    LOCATION        = 'daily_summary',
    FILE_FORMAT     = ff_delta_parquet
);
GO

-- ── Ops — Data quality metrics per micro-batch ────────────────────────────────
IF OBJECT_ID('ops.dq_metrics', 'ET') IS NOT NULL DROP EXTERNAL TABLE ops.dq_metrics;

CREATE EXTERNAL TABLE ops.dq_metrics
(
    [table_name]          NVARCHAR(200),
    [batch_id]            BIGINT,
    [run_id]              NVARCHAR(100),
    [rows_in]             BIGINT,
    [rows_valid]          BIGINT,
    [rows_quarantined]    BIGINT,
    [rows_deduplicated]   BIGINT,
    [quarantine_rate]     FLOAT,
    [measured_at]         DATETIME2(7)
)
WITH (
    DATA_SOURCE     = eds_silver,
    LOCATION        = '_ops/dq_metrics',
    FILE_FORMAT     = ff_delta_parquet
);
GO

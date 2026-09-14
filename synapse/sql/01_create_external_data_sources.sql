-- =============================================================================
-- 01 — External Data Sources pointing to ADLS Gen2 containers
-- Replace: stfinancialdata  ->  your actual storage account name
-- =============================================================================

USE financial_analytics;
GO

-- ── Bronze ────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'eds_bronze')
    CREATE EXTERNAL DATA SOURCE eds_bronze
    WITH (
        LOCATION   = 'abfss://bronze@stfinancialdata.dfs.core.windows.net',
        CREDENTIAL = managed_identity_credential
    );
GO

-- ── Silver ────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'eds_silver')
    CREATE EXTERNAL DATA SOURCE eds_silver
    WITH (
        LOCATION   = 'abfss://silver@stfinancialdata.dfs.core.windows.net',
        CREDENTIAL = managed_identity_credential
    );
GO

-- ── Gold ──────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'eds_gold')
    CREATE EXTERNAL DATA SOURCE eds_gold
    WITH (
        LOCATION   = 'abfss://gold@stfinancialdata.dfs.core.windows.net',
        CREDENTIAL = managed_identity_credential
    );
GO

-- ── File format: Delta Lake (read as Parquet in serverless SQL) ───────────────
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'ff_delta_parquet')
    CREATE EXTERNAL FILE FORMAT ff_delta_parquet
    WITH (FORMAT_TYPE = DELTA);
GO

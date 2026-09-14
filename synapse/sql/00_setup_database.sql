-- =============================================================================
-- 00 — Setup Database and Managed Identity Credential
-- Run as: Synapse workspace admin on the built-in (serverless) SQL pool
-- =============================================================================

-- Create dedicated database for financial analytics
-- (queries run on serverless, but logical db groups objects cleanly)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'financial_analytics')
    CREATE DATABASE financial_analytics;
GO

USE financial_analytics;
GO

-- Create database-scoped credential using the Synapse Managed Identity
-- Grant Storage Blob Data Reader on the ADLS account to the Synapse MI
IF NOT EXISTS (
    SELECT 1 FROM sys.database_scoped_credentials
    WHERE name = 'managed_identity_credential'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL managed_identity_credential
    WITH IDENTITY = 'Managed Identity';
END
GO

-- Create schemas matching medallion layers
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')   EXEC('CREATE SCHEMA gold');
GO

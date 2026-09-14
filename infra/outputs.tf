output "resource_group_name" {
  description = "Name of the Azure Resource Group"
  value       = azurerm_resource_group.rg.name
}

output "adls_account_name" {
  description = "Azure Data Lake Storage Gen2 account name"
  value       = module.storage.storage_account_name
}

output "adls_dfs_endpoint" {
  description = "ADLS Gen2 DFS endpoint (for abfss:// paths)"
  value       = module.storage.primary_dfs_endpoint
}

output "adf_name" {
  description = "Azure Data Factory name"
  value       = module.adf.adf_name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL (use to configure DATABRICKS_HOST)"
  value       = module.databricks.workspace_url
}

output "synapse_sql_endpoint" {
  description = "Synapse serverless SQL endpoint (use in .env SYNAPSE_SQL_ENDPOINT)"
  value       = module.synapse.synapse_connectivity_endpoints
}

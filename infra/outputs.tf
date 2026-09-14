output "resource_group_name" {
  description = "Nome do Resource Group Azure"
  value       = azurerm_resource_group.rg.name
}

output "adls_account_name" {
  description = "Nome da conta Azure Data Lake Storage Gen2"
  value       = module.storage.storage_account_name
}

output "adls_dfs_endpoint" {
  description = "Endpoint DFS do ADLS Gen2 (para caminhos abfss://)"
  value       = module.storage.primary_dfs_endpoint
}

output "adf_name" {
  description = "Nome do Azure Data Factory"
  value       = module.adf.adf_name
}

output "databricks_workspace_url" {
  description = "URL do workspace Databricks (use para configurar DATABRICKS_HOST)"
  value       = module.databricks.workspace_url
}

output "synapse_sql_endpoint" {
  description = "Endpoint SQL serverless do Synapse (use em .env como SYNAPSE_SQL_ENDPOINT)"
  value       = module.synapse.synapse_connectivity_endpoints
}

terraform {
  required_providers {
    azurerm  = { source = "hashicorp/azurerm",  version = "~> 3.100" }
    databricks = { source = "databricks/databricks", version = "~> 1.40" }
  }
}

variable "resource_group_name"  { type = string }
variable "location"             { type = string }
variable "workspace_name"       { type = string }
variable "sku"                  { type = string }
variable "storage_account_name" { type = string }
variable "tags"                 { type = map(string) }

resource "azurerm_databricks_workspace" "dbw" {
  name                        = var.workspace_name
  location                    = var.location
  resource_group_name         = var.resource_group_name
  sku                         = var.sku
  managed_resource_group_name = "${var.resource_group_name}-dbr-managed"
  tags                        = var.tags
}

# Concede Storage Blob Data Contributor ao managed identity do Databricks no ADLS
data "azurerm_storage_account" "adls" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_role_assignment" "dbr_adls_contributor" {
  scope                = data.azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.dbw.storage_account_identity[0].principal_id
}

output "workspace_url"  { value = azurerm_databricks_workspace.dbw.workspace_url }
output "workspace_id"   { value = azurerm_databricks_workspace.dbw.id }

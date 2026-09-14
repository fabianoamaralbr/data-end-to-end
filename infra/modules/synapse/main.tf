terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
  }
}

variable "resource_group_name"   { type = string }
variable "location"              { type = string }
variable "workspace_name"        { type = string }
variable "storage_account_id"    { type = string }
variable "storage_account_name"  { type = string }
variable "sql_admin_login"       { type = string,  sensitive = true }
variable "sql_admin_password"    { type = string,  sensitive = true }
variable "tags"                  { type = map(string) }

resource "azurerm_synapse_workspace" "synw" {
  name                                 = var.workspace_name
  location                             = var.location
  resource_group_name                  = var.resource_group_name
  storage_data_lake_gen2_filesystem_id = "${var.storage_account_id}/blobServices/default/containers/synapse-root"
  sql_administrator_login              = var.sql_admin_login
  sql_administrator_login_password     = var.sql_admin_password
  tags                                 = var.tags

  identity {
    type = "SystemAssigned"
  }
}

# Permite que o Synapse serverless SQL leia as tabelas Delta no ADLS
data "azurerm_storage_account" "adls" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_role_assignment" "synapse_adls_reader" {
  scope                = data.azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_synapse_workspace.synw.identity[0].principal_id
}

# Regra de firewall — abre para servicos Azure (necessario para o trigger do ADF)
resource "azurerm_synapse_firewall_rule" "allow_azure_services" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.synw.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

output "synapse_workspace_id"            { value = azurerm_synapse_workspace.synw.id }
output "synapse_connectivity_endpoints"  { value = azurerm_synapse_workspace.synw.connectivity_endpoints }

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
  }
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "workspace_name" { type = string }
variable "sku" { type = string }
variable "storage_account_id" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_databricks_workspace" "dbw" {
  name                        = var.workspace_name
  location                    = var.location
  resource_group_name         = var.resource_group_name
  sku                         = var.sku
  managed_resource_group_name = "${var.resource_group_name}-dbr-managed"
  tags                        = var.tags
}

# Access connector: managed identity que o Unity Catalog usa para acessar o ADLS.
# Substitui mounts no DBFS + service principal com client secret (ambos depreciados):
# nao ha segredo para rotacionar nem credencial exposta ao codigo dos notebooks.
resource "azurerm_databricks_access_connector" "uc" {
  name                = "${var.workspace_name}-uc-connector"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_adls_contributor" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

output "workspace_url" { value = azurerm_databricks_workspace.dbw.workspace_url }
output "workspace_id" { value = azurerm_databricks_workspace.dbw.id }
output "access_connector_id" { value = azurerm_databricks_access_connector.uc.id }
output "access_connector_role_id" { value = azurerm_role_assignment.uc_adls_contributor.id }

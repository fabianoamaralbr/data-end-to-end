terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
  }
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "factory_name" { type = string }
variable "storage_account_name" { type = string }
variable "github_account_name" {
  type    = string
  default = ""
}
variable "tags" { type = map(string) }

resource "azurerm_data_factory" "adf" {
  name                = var.factory_name
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }

  # Integracao Git do ADF (publica a partir de adf/ no repositorio). Opcional: so e
  # criada quando a conta GitHub e informada.
  dynamic "github_configuration" {
    for_each = var.github_account_name == "" ? [] : [1]
    content {
      account_name    = var.github_account_name
      branch_name     = "main"
      git_url         = "https://github.com"
      repository_name = "data-end-to-end"
      root_folder     = "/adf"
    }
  }
}

# Concede Storage Blob Data Contributor ao managed identity do ADF no ADLS
data "azurerm_storage_account" "adls" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_role_assignment" "adf_adls_contributor" {
  scope                = data.azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

output "adf_id" { value = azurerm_data_factory.adf.id }
output "adf_name" { value = azurerm_data_factory.adf.name }
output "adf_principal_id" { value = azurerm_data_factory.adf.identity[0].principal_id }

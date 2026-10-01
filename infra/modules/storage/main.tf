terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
  }
}

variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "account_name" { type = string }
variable "account_tier" { type = string }
variable "replication_type" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_storage_account" "adls" {
  name                     = var.account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = var.account_tier
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true # habilita o namespace hierarquico do ADLS Gen2

  blob_properties {
    versioning_enabled = true
    delete_retention_policy { days = 7 }
  }

  tags = var.tags
}

locals {
  # catalog: managed storage do catalogo Unity Catalog
  # synapse-root: filesystem padrao exigido pelo workspace Synapse
  containers = ["raw", "bronze", "silver", "gold", "catalog", "synapse-root"]
}

resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(local.containers)
  name               = each.key
  storage_account_id = azurerm_storage_account.adls.id
}

output "storage_account_id" { value = azurerm_storage_account.adls.id }
output "storage_account_name" { value = azurerm_storage_account.adls.name }
output "primary_dfs_endpoint" { value = azurerm_storage_account.adls.primary_dfs_endpoint }
output "containers" { value = local.containers }
output "synapse_filesystem_id" { value = azurerm_storage_data_lake_gen2_filesystem.layers["synapse-root"].id }

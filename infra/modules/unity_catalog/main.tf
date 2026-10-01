terraform {
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.40" }
  }
}

variable "storage_account_name" { type = string }
variable "access_connector_id" { type = string }
variable "catalog_name" { type = string }
variable "containers" { type = list(string) }

locals {
  layer_containers = toset([for c in var.containers : c if c != "synapse-root"])
  schemas          = toset(["bronze", "silver", "gold", "ops"])
}

resource "databricks_storage_credential" "adls" {
  name    = "sc-${var.storage_account_name}"
  comment = "Managed identity do access connector do workspace"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "layers" {
  for_each        = local.layer_containers
  name            = "el-${var.storage_account_name}-${each.key}"
  url             = "abfss://${each.key}@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.name
  comment         = "Container ${each.key} do pipeline medallion"
}

resource "databricks_catalog" "financial" {
  name         = var.catalog_name
  storage_root = databricks_external_location.layers["catalog"].url
  comment      = "Plataforma de dados financeiros (Kaggle OHLCV + Banco Central SGS)"
}

resource "databricks_schema" "layers" {
  for_each     = local.schemas
  catalog_name = databricks_catalog.financial.name
  name         = each.key
}

output "catalog_name" { value = databricks_catalog.financial.name }
output "raw_external_location_url" { value = databricks_external_location.layers["raw"].url }

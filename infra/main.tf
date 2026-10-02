terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }

  # Configuracao parcial: os valores vem de `-backend-config=backend.hcl` (local) ou
  # dos secrets do GitHub (CI). Ver backend.hcl.example e scripts/bootstrap_tfstate.sh.
  backend "azurerm" {}
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  azure_workspace_resource_id = module.databricks.workspace_id
}

locals {
  prefix = "${var.project}-${var.environment}"
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.prefix}"
  location = var.location
  tags     = var.tags
}

module "storage" {
  source = "./modules/storage"

  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  account_name        = "st${replace(local.prefix, "-", "")}"
  account_tier        = var.adls_account_tier
  replication_type    = var.adls_replication
  tags                = var.tags
}

module "adf" {
  source = "./modules/adf"

  resource_group_name  = azurerm_resource_group.rg.name
  location             = var.location
  factory_name         = "adf-${local.prefix}"
  storage_account_name = module.storage.storage_account_name
  github_account_name  = var.adf_github_account_name
  tags                 = var.tags
}

module "databricks" {
  source = "./modules/databricks"

  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  workspace_name      = "dbw-${local.prefix}"
  sku                 = var.databricks_sku
  storage_account_id  = module.storage.storage_account_id
  tags                = var.tags
}

module "unity_catalog" {
  source = "./modules/unity_catalog"

  storage_account_name = module.storage.storage_account_name
  access_connector_id  = module.databricks.access_connector_id
  catalog_name         = var.uc_catalog_name
  containers           = module.storage.containers

  # O RBAC do access connector precisa existir antes de validar as external locations.
  depends_on = [module.databricks]
}

module "synapse" {
  source = "./modules/synapse"

  resource_group_name  = azurerm_resource_group.rg.name
  location             = var.location
  workspace_name       = "synw-${local.prefix}"
  storage_account_id   = module.storage.storage_account_id
  storage_account_name = module.storage.storage_account_name
  filesystem_id        = module.storage.synapse_filesystem_id
  sql_admin_login      = var.synapse_sql_admin_login
  sql_admin_password   = var.synapse_sql_admin_password
  tags                 = var.tags
}

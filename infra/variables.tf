variable "project" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "financial-data"
}

variable "environment" {
  description = "Deployment environment: dev | stg | prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "stg", "prod"], var.environment)
    error_message = "environment must be one of: dev, stg, prod"
  }
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus2"
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    project     = "financial-data"
    team        = "data-engineering"
    managed_by  = "terraform"
  }
}

# ── Storage ───────────────────────────────────────────────────────────────────
variable "adls_account_tier" {
  description = "ADLS Gen2 storage account tier"
  type        = string
  default     = "Standard"
}

variable "adls_replication" {
  description = "ADLS Gen2 replication type"
  type        = string
  default     = "LRS"
}

# ── Databricks ────────────────────────────────────────────────────────────────
variable "databricks_sku" {
  description = "Databricks workspace SKU: standard | premium | trial"
  type        = string
  default     = "premium"
}

# ── Synapse ───────────────────────────────────────────────────────────────────
variable "synapse_sql_admin_login" {
  description = "Synapse SQL admin username"
  type        = string
  default     = "sqladminuser"
  sensitive   = true
}

variable "synapse_sql_admin_password" {
  description = "Synapse SQL admin password (minimum 8 chars, uppercase, lowercase, digit, special)"
  type        = string
  sensitive   = true
}

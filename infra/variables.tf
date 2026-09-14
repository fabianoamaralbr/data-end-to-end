variable "project" {
  description = "Nome do projeto usado como prefixo em todos os recursos"
  type        = string
  default     = "financial-data"
}

variable "environment" {
  description = "Ambiente de implantacao: dev | stg | prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "stg", "prod"], var.environment)
    error_message = "O ambiente deve ser um dos seguintes: dev, stg, prod"
  }
}

variable "location" {
  description = "Regiao Azure para todos os recursos"
  type        = string
  default     = "eastus2"
}

variable "tags" {
  description = "Tags aplicadas a todos os recursos"
  type        = map(string)
  default = {
    project     = "financial-data"
    team        = "data-engineering"
    managed_by  = "terraform"
  }
}

# ── Storage ───────────────────────────────────────────────────────────────────
variable "adls_account_tier" {
  description = "Tier da conta de armazenamento ADLS Gen2"
  type        = string
  default     = "Standard"
}

variable "adls_replication" {
  description = "Tipo de replicacao do ADLS Gen2"
  type        = string
  default     = "LRS"
}

# ── Databricks ────────────────────────────────────────────────────────────────
variable "databricks_sku" {
  description = "SKU do workspace Databricks: standard | premium | trial"
  type        = string
  default     = "premium"
}

# ── Synapse ───────────────────────────────────────────────────────────────────
variable "synapse_sql_admin_login" {
  description = "Usuario administrador do Synapse SQL"
  type        = string
  default     = "sqladminuser"
  sensitive   = true
}

variable "synapse_sql_admin_password" {
  description = "Senha do administrador Synapse SQL (minimo 8 caracteres, maiuscula, minuscula, digito e especial)"
  type        = string
  sensitive   = true
}

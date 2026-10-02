# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup das tabelas no Unity Catalog
# MAGIC
# MAGIC Cria (se nao existirem) todas as tabelas do pipeline como **tabelas externas do
# MAGIC Unity Catalog**, com o DDL gerado a partir dos contratos em `src/transforms/contracts.py`.
# MAGIC
# MAGIC Se uma tabela ja existir com schema diferente do contrato, o notebook **falha** com
# MAGIC `SchemaContractError` — mudanca de schema exige mudanca versionada no contrato.
# MAGIC
# MAGIC Pre-requisitos (provisionados pelo Terraform, `infra/modules/unity_catalog`):
# MAGIC access connector (managed identity), storage credential, external locations,
# MAGIC catalogo e schemas. Nao ha mounts nem secrets de service principal.

# COMMAND ----------

import os
import sys

# Notebooks em Git folders rodam com o diretorio do notebook como CWD.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from transforms.datasets import ALL_TABLES
from transforms.tables import ensure_table

# COMMAND ----------

dbutils.widgets.text("catalog", "financial")
dbutils.widgets.text("storage_account", "")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
assert STORAGE_ACCOUNT, "parametro storage_account e obrigatorio"

# COMMAND ----------

for schema in ("bronze", "silver", "gold", "ops"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

for spec in ALL_TABLES:
    name = spec.full_name(CATALOG)
    ensure_table(spark, name, spec.contract, location=spec.location(STORAGE_ACCOUNT), properties=spec.properties)
    print(f"[setup] ok  {name}  ->  {spec.location(STORAGE_ACCOUNT)}")

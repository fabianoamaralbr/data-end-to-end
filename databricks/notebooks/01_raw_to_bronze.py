# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Raw para Bronze
# MAGIC
# MAGIC **Camada:** Bronze (ingestao bruta no Delta Lake)
# MAGIC
# MAGIC Le os arquivos CSV do container `raw`, adiciona colunas de metadados de ingestao
# MAGIC e grava no container `bronze` no formato Delta Lake.
# MAGIC Sem logica de negocio — os dados chegam exatamente como foram ingeridos.
# MAGIC
# MAGIC | Metrica | Descricao |
# MAGIC |---------|-----------|
# MAGIC | rows_read | Total de linhas lidas do CSV |
# MAGIC | rows_written | Linhas gravadas no Delta bronze |
# MAGIC | columns_in | Quantidade de colunas do CSV |
# MAGIC | columns_out | Quantidade de colunas no Delta bronze |

# COMMAND ----------

import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# COMMAND ----------

spark = SparkSession.builder.appName("raw_to_bronze").getOrCreate()

spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# COMMAND ----------

RAW_PATH    = "/mnt/raw/financial-data/"
BRONZE_PATH = "/mnt/bronze/financial_data/"
RUN_TS      = datetime.now(timezone.utc).isoformat()

# COMMAND ----------
# MAGIC %md ## 1. Leitura do CSV bruto

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(RAW_PATH)
)

rows_read    = raw_df.count()
columns_in   = len(raw_df.columns)

print(f"[bronze] rows_read={rows_read}  columns_in={columns_in}")

# COMMAND ----------
# MAGIC %md ## 2. Adicao de colunas de metadados de ingestao

bronze_df = (
    raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_pipeline_run_ts", lit(RUN_TS))
)

columns_out = len(bronze_df.columns)

# COMMAND ----------
# MAGIC %md ## 3. Gravacao no Delta Lake (append — suporta execucoes incrementais)

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

rows_written = spark.read.format("delta").load(BRONZE_PATH).count()

print(f"[bronze] rows_written={rows_written}  columns_out={columns_out}")
print(f"[bronze] CONCLUIDO — tabela Delta em {BRONZE_PATH}")

# COMMAND ----------
# MAGIC %md ## 4. Verificacao rapida de qualidade

bronze_latest = spark.read.format("delta").load(BRONZE_PATH)
display(bronze_latest.limit(5))

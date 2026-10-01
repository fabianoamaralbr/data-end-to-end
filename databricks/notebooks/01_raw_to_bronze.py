# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Raw para Bronze (incremental e idempotente)
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) descobre apenas arquivos novos/alterados no container
# MAGIC   `raw`; o checkpoint registra o que ja foi processado.
# MAGIC - Tudo e lido como **string** com schema explicito; colunas inesperadas vao para
# MAGIC   `_rescued_data` (e depois para a quarentena da silver), nunca evoluem o schema.
# MAGIC - Cada registro recebe uma **chave de ingestao** (hash de arquivo + conteudo) e entra
# MAGIC   via `MERGE` insert-only: reexecutar a ingestao, ou perder o checkpoint, nao duplica dados.
# MAGIC
# MAGIC Logica em `src/transforms/bronze.py` e `src/transforms/batch.py` (coberta por testes).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from transforms.batch import process_bronze_batch
from transforms.datasets import abfss, get_dataset

# COMMAND ----------

dbutils.widgets.text("catalog", "financial")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.dropdown("dataset", "ohlcv", ["ohlcv", "bcb_sgs"])
dbutils.widgets.text("run_id", "manual")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
DATASET = get_dataset(dbutils.widgets.get("dataset"))
RUN_ID = dbutils.widgets.get("run_id")

RAW_PATH = abfss(STORAGE_ACCOUNT, "raw", DATASET.raw_path)
CHECKPOINT = DATASET.checkpoint(STORAGE_ACCOUNT, "bronze")
TARGET = DeltaTable.forName(spark, DATASET.bronze.full_name(CATALOG))

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", DATASET.raw_format)
    .options(**DATASET.raw_options)
    .option("rescuedDataColumn", "_rescued_data")
    # Arquivo reescrito na origem (ex.: janela do BCB reprocessada) e lido de novo;
    # a chave de ingestao descarta o que nao mudou.
    .option("cloudFiles.allowOverwrites", "true")
    .schema(DATASET.raw_schema)
    .load(RAW_PATH)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_modified_at"),
    )
)

query = (
    raw_stream.writeStream
    .foreachBatch(
        lambda batch_df, batch_id: process_bronze_batch(
            batch_df, batch_id, dataset=DATASET, target=TARGET, run_id=RUN_ID
        )
    )
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

progress = query.recentProgress
rows_read = sum(p["numInputRows"] for p in progress)
print(f"[bronze:{DATASET.name}] batches={len(progress)} rows_read={rows_read} run_id={RUN_ID}")

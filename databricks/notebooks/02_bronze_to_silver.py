# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze para Silver (incremental, com quarentena e circuit breaker)
# MAGIC
# MAGIC Le **somente os registros novos** da bronze (stream Delta com checkpoint) e, por micro-batch:
# MAGIC
# MAGIC 1. valida o batch contra o contrato da bronze e tipa as colunas (`try_cast`);
# MAGIC 2. separa os registros invalidos na **tabela de quarentena**, com o motivo da rejeicao;
# MAGIC 3. deduplica de forma **deterministica** (window ordenada por ingestao, nao `dropDuplicates`);
# MAGIC 4. grava as metricas do batch em `ops.dq_metrics`;
# MAGIC 5. **falha o job** se a taxa de quarentena passar de `max_quarantine_rate`;
# MAGIC 6. faz `MERGE` na silver por chave de negocio, mantendo sempre a versao mais recente.
# MAGIC
# MAGIC Sem `overwrite`, sem `overwriteSchema`, sem particionamento por data (o layout e
# MAGIC otimizado com `ZORDER`). Logica em `src/transforms/` (coberta por testes).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from delta.tables import DeltaTable

from transforms.batch import process_silver_batch
from transforms.datasets import DQ_METRICS, get_dataset
from transforms.tables import optimize

# COMMAND ----------

dbutils.widgets.text("catalog", "financial")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.dropdown("dataset", "ohlcv", ["ohlcv", "bcb_sgs"])
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("max_quarantine_rate", "0.05")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
DATASET = get_dataset(dbutils.widgets.get("dataset"))
RUN_ID = dbutils.widgets.get("run_id")
MAX_QUARANTINE_RATE = float(dbutils.widgets.get("max_quarantine_rate"))

SILVER = DATASET.silver.full_name(CATALOG)
tables = {
    "silver": DeltaTable.forName(spark, SILVER),
    "quarantine": DeltaTable.forName(spark, DATASET.quarantine.full_name(CATALOG)),
    "metrics": DeltaTable.forName(spark, DQ_METRICS.full_name(CATALOG)),
}

# COMMAND ----------

query = (
    spark.readStream.table(DATASET.bronze.full_name(CATALOG))
    .writeStream
    .foreachBatch(
        lambda batch_df, batch_id: process_silver_batch(
            batch_df, batch_id, dataset=DATASET, run_id=RUN_ID,
            max_quarantine_rate=MAX_QUARANTINE_RATE, **tables,
        )
    )
    .option("checkpointLocation", DATASET.checkpoint(STORAGE_ACCOUNT, "silver"))
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

optimize(spark, SILVER, DATASET.silver.zorder_by)

display(
    spark.table(DQ_METRICS.full_name(CATALOG))
    .filter(f"table_name = '{DATASET.silver.name}' AND run_id = '{RUN_ID}'")
    .orderBy("batch_id")
)

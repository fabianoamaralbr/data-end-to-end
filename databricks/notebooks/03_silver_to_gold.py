# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver para Gold (recalculo apenas dos simbolos afetados)
# MAGIC
# MAGIC Le o **Change Data Feed** da silver e, por micro-batch, recalcula o historico completo
# MAGIC apenas dos simbolos que mudaram (medias moveis e retornos acumulados dependem do
# MAGIC historico inteiro do simbolo). A fatia de cada simbolo e substituida atomicamente via
# MAGIC `MERGE ... WHEN NOT MATCHED BY SOURCE DELETE` — sem overwrite da tabela inteira.
# MAGIC
# MAGIC | Coluna | Logica |
# MAGIC |--------|--------|
# MAGIC | ma_close_7d / ma_close_30d | Media movel simples de 7 / 30 pregoes do Close |
# MAGIC | ma_volume_7d | Media movel de 7 pregoes do Volume |
# MAGIC | prev_close | Close do pregao anterior (lag 1) |
# MAGIC | day_over_day_return_pct | (Close - prev_close) / prev_close * 100 |
# MAGIC | cumulative_return_pct | Retorno acumulado desde o primeiro registro do simbolo |
# MAGIC
# MAGIC Logica em `src/transforms/gold.py` e `src/transforms/batch.py` (coberta por testes).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))

from delta.tables import DeltaTable

from transforms.batch import process_gold_batch
from transforms.datasets import GOLD_OHLCV_DAILY_SUMMARY, GOLD_OHLCV_DETAIL, OHLCV
from transforms.tables import optimize

# COMMAND ----------

dbutils.widgets.text("catalog", "financial")
dbutils.widgets.text("storage_account", "")

CATALOG = dbutils.widgets.get("catalog")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

SILVER = OHLCV.silver.full_name(CATALOG)
DETAIL = GOLD_OHLCV_DETAIL.full_name(CATALOG)
SUMMARY = GOLD_OHLCV_DAILY_SUMMARY.full_name(CATALOG)

# COMMAND ----------

symbols_processed = []


def _process(changes_df, batch_id):
    symbols_processed.extend(
        process_gold_batch(
            changes_df, batch_id, silver_table=SILVER,
            detail=DeltaTable.forName(spark, DETAIL), summary=DeltaTable.forName(spark, SUMMARY),
        )
    )


query = (
    spark.readStream.option("readChangeFeed", "true").table(SILVER)
    .writeStream
    .foreachBatch(_process)
    .option("checkpointLocation", OHLCV.checkpoint(STORAGE_ACCOUNT, "gold"))
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print(f"[gold] simbolos recalculados: {sorted(set(symbols_processed))}")

# COMMAND ----------

if symbols_processed:
    for spec in (GOLD_OHLCV_DETAIL, GOLD_OHLCV_DAILY_SUMMARY):
        optimize(spark, spec.full_name(CATALOG), spec.zorder_by)

display(spark.table(SUMMARY).orderBy("Symbol", "Date").limit(10))

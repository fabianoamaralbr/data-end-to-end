# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Raw to Bronze
# MAGIC
# MAGIC **Layer:** Bronze (raw ingestion into Delta Lake)
# MAGIC
# MAGIC Reads CSV files from the `raw` container, adds ingestion metadata columns,
# MAGIC and writes to the `bronze` container in Delta Lake format.
# MAGIC No business logic here — data arrives exactly as ingested.
# MAGIC
# MAGIC | Metric | Description |
# MAGIC |--------|-------------|
# MAGIC | rows_read | Total rows from CSV |
# MAGIC | rows_written | Rows committed to bronze Delta |
# MAGIC | columns_in | CSV column count |
# MAGIC | columns_out | Bronze Delta column count |

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
# MAGIC %md ## 1. Read raw CSV

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
# MAGIC %md ## 2. Add ingestion metadata columns

bronze_df = (
    raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_pipeline_run_ts", lit(RUN_TS))
)

columns_out = len(bronze_df.columns)

# COMMAND ----------
# MAGIC %md ## 3. Write to Delta Lake (append — supports incremental runs)

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

rows_written = spark.read.format("delta").load(BRONZE_PATH).count()

print(f"[bronze] rows_written={rows_written}  columns_out={columns_out}")
print(f"[bronze] DONE — Delta table at {BRONZE_PATH}")

# COMMAND ----------
# MAGIC %md ## 4. Quick quality check

bronze_latest = spark.read.format("delta").load(BRONZE_PATH)
display(bronze_latest.limit(5))

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze to Silver
# MAGIC
# MAGIC **Layer:** Silver (cleansed, validated, enriched)
# MAGIC
# MAGIC Transformations applied:
# MAGIC - Cast all columns to correct types (Date, Double, Long)
# MAGIC - Drop exact duplicates on (Date, Symbol)
# MAGIC - Filter rows with null or non-positive Close price
# MAGIC - Derive `daily_return_pct` = (Close - Open) / Open * 100
# MAGIC - Derive `intraday_range`   = High - Low
# MAGIC - Derive `price_spread_pct` = (High - Low) / Open * 100
# MAGIC - Partition by date for efficient downstream querying

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    round as spark_round,
    to_date,
)

# COMMAND ----------

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# COMMAND ----------

BRONZE_PATH = "/mnt/bronze/financial_data/"
SILVER_PATH = "/mnt/silver/financial_data/"

# COMMAND ----------
# MAGIC %md ## 1. Read bronze Delta

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
rows_bronze = bronze_df.count()
print(f"[silver] rows_bronze={rows_bronze}")

# COMMAND ----------
# MAGIC %md ## 2. Cast types

typed_df = (
    bronze_df
    .withColumn("Date",   to_date(col("Date"), "yyyy-MM-dd"))
    .withColumn("Open",   col("Open").cast("double"))
    .withColumn("High",   col("High").cast("double"))
    .withColumn("Low",    col("Low").cast("double"))
    .withColumn("Close",  col("Close").cast("double"))
    .withColumn("Volume", col("Volume").cast("long"))
    .withColumn("OpenInt", col("OpenInt").cast("long"))
)

# COMMAND ----------
# MAGIC %md ## 3. Deduplicate + filter invalid rows

clean_df = (
    typed_df
    .dropDuplicates(["Date", "Symbol"])
    .filter(col("Close").isNotNull() & (col("Close") > 0))
    .filter(col("Open").isNotNull()  & (col("Open")  > 0))
    .filter(col("Date").isNotNull())
)

rows_clean = clean_df.count()
rows_dropped = rows_bronze - rows_clean
print(f"[silver] rows_clean={rows_clean}  rows_dropped={rows_dropped}")

# COMMAND ----------
# MAGIC %md ## 4. Derive business metrics

silver_df = (
    clean_df
    .withColumn(
        "daily_return_pct",
        spark_round((col("Close") - col("Open")) / col("Open") * 100, 4)
    )
    .withColumn(
        "intraday_range",
        spark_round(col("High") - col("Low"), 4)
    )
    .withColumn(
        "price_spread_pct",
        spark_round((col("High") - col("Low")) / col("Open") * 100, 4)
    )
    .withColumn("_transformed_at", current_timestamp())
)

# COMMAND ----------
# MAGIC %md ## 5. Write Silver Delta (overwrite with schema evolution, partition by Date)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("Date")
    .save(SILVER_PATH)
)

rows_silver = spark.read.format("delta").load(SILVER_PATH).count()
print(f"[silver] rows_written={rows_silver}  DONE")

display(silver_df.orderBy("Date", "Symbol").limit(10))

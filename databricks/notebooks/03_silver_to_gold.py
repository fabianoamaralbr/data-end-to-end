# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver para Gold
# MAGIC
# MAGIC **Camada:** Gold (agregacoes prontas para o negocio)
# MAGIC
# MAGIC Produz duas tabelas Gold:
# MAGIC - **`financial_data`** — linha a linha com medias moveis (MA7, MA30) e retorno com lag
# MAGIC - **`daily_summary`** — metricas diarias agregadas por simbolo para Power BI e Synapse SQL
# MAGIC
# MAGIC | Coluna | Logica |
# MAGIC |--------|--------|
# MAGIC | ma_close_7d | Media movel simples de 7 dias do Close |
# MAGIC | ma_close_30d | Media movel simples de 30 dias do Close |
# MAGIC | ma_volume_7d | Media movel de 7 dias do Volume |
# MAGIC | prev_close | Close do pregao anterior (lag 1) |
# MAGIC | day_over_day_return_pct | (Close - prev_close) / prev_close * 100 |
# MAGIC | cumulative_return_pct | Retorno acumulado (%) desde o primeiro registro do simbolo |

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    first,
    lag,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.window import Window

# COMMAND ----------

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# COMMAND ----------

SILVER_PATH     = "/mnt/silver/financial_data/"
GOLD_DETAIL_PATH = "/mnt/gold/financial_data/"
GOLD_SUMMARY_PATH = "/mnt/gold/daily_summary/"

# COMMAND ----------
# MAGIC %md ## 1. Leitura do Silver

silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"[gold] rows_silver={silver_df.count()}")

# COMMAND ----------
# MAGIC %md ## 2. Funcoes de janela — medias moveis e lag

window_symbol_date = Window.partitionBy("Symbol").orderBy("Date")
window_7d          = window_symbol_date.rowsBetween(-6, 0)
window_30d         = window_symbol_date.rowsBetween(-29, 0)

first_record_window = Window.partitionBy("Symbol").orderBy("Date")

detail_df = (
    silver_df
    .withColumn("ma_close_7d",   spark_round(avg("Close").over(window_7d), 4))
    .withColumn("ma_close_30d",  spark_round(avg("Close").over(window_30d), 4))
    .withColumn("ma_volume_7d",  spark_round(avg("Volume").over(window_7d), 0).cast("long"))
    .withColumn("prev_close",    lag("Close", 1).over(window_symbol_date))
    .withColumn(
        "day_over_day_return_pct",
        spark_round(
            (col("Close") - col("prev_close")) / col("prev_close") * 100, 4
        )
    )
    .withColumn("first_close",   first("Close").over(first_record_window))
    .withColumn(
        "cumulative_return_pct",
        spark_round(
            (col("Close") - col("first_close")) / col("first_close") * 100, 4
        )
    )
    .drop("first_close")
    .withColumn("_gold_created_at", current_timestamp())
)

# COMMAND ----------
# MAGIC %md ## 3. Gravacao da tabela Gold detalhada

(
    detail_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("Symbol")
    .save(GOLD_DETAIL_PATH)
)

print(f"[gold] detail rows_written={spark.read.format('delta').load(GOLD_DETAIL_PATH).count()}")

# COMMAND ----------
# MAGIC %md ## 4. Agregacao do resumo diario por simbolo

summary_df = (
    detail_df
    .groupBy("Symbol", "Date")
    .agg(
        first("Open").alias("open"),
        spark_max("High").alias("high"),
        spark_min("Low").alias("low"),
        first("Close").alias("close"),
        spark_sum("Volume").alias("total_volume"),
        avg("daily_return_pct").alias("daily_return_pct"),
        first("ma_close_7d").alias("ma_close_7d"),
        first("ma_close_30d").alias("ma_close_30d"),
        first("day_over_day_return_pct").alias("day_over_day_return_pct"),
        first("cumulative_return_pct").alias("cumulative_return_pct"),
        count("*").alias("row_count"),
    )
    .orderBy("Symbol", "Date")
)

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("Symbol")
    .save(GOLD_SUMMARY_PATH)
)

print(f"[gold] summary rows_written={spark.read.format('delta').load(GOLD_SUMMARY_PATH).count()}")
print("[gold] DONE")

display(summary_df.limit(10))

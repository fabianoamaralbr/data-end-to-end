# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup ADLS Mounts
# MAGIC
# MAGIC Mounts all four ADLS Gen2 containers (raw, bronze, silver, gold) to DBFS.
# MAGIC Run once per cluster restart or use cluster init scripts.
# MAGIC
# MAGIC **Prerequisites:** Service principal credentials stored in Databricks secrets
# MAGIC under scope `financial-data`.

# COMMAND ----------

import os

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account") if dbutils.widgets.getArgument("storage_account", "") else os.environ.get("ADLS_ACCOUNT_NAME", "stfinancialdata")
TENANT_ID       = dbutils.secrets.get(scope="financial-data", key="tenant-id")
CLIENT_ID       = dbutils.secrets.get(scope="financial-data", key="client-id")
CLIENT_SECRET   = dbutils.secrets.get(scope="financial-data", key="client-secret")

CONTAINERS = ["raw", "bronze", "silver", "gold"]

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": CLIENT_ID,
    "fs.azure.account.oauth2.client.secret": CLIENT_SECRET,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token",
}

for container in CONTAINERS:
    mount_point = f"/mnt/{container}"
    source      = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

    existing_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    if mount_point in existing_mounts:
        dbutils.fs.unmount(mount_point)

    dbutils.fs.mount(
        source=source,
        mount_point=mount_point,
        extra_configs=configs,
    )
    print(f"Mounted {source} -> {mount_point}")

# COMMAND ----------

display(dbutils.fs.mounts())

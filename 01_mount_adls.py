# Databricks notebook source
storage_account_name = "stdatapipelineadam"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net",
    storage_account_key
)

print("done")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@stdatapipelineadam.dfs.core.windows.net/")
# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Access Keys
# MAGIC 1. Set Spark config `fs.azure.key`
# MAGIC 2. List files from demo cluster
# MAGIC 3. Read data from `circuits.csv`

# COMMAND ----------

# KEY SHOULD BE PLACED IN CLUSTER ADVNACED OPTIONS

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@f1bonamdll.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1bonamdll.dfs.core.windows.net/circuits.csv"))

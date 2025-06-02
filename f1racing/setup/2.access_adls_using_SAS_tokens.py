# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS token
# MAGIC 1. Set Spark config sas token
# MAGIC 2. List files from demo cluster
# MAGIC 3. Read data from `circuits.csv`

# COMMAND ----------

f1racingToken=dbutils.secrets.get(scope='fromula1scope',key='tokenKeyf1')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.f1bonamdll.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1bonamdll.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1bonamdll.dfs.core.windows.net",f1racingToken)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@f1bonamdll.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1bonamdll.dfs.core.windows.net/circuits.csv"))

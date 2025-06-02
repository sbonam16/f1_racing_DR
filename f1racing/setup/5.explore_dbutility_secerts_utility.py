# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list(scope='fromula1scope')

# COMMAND ----------

dbutils.secrets.get(scope='fromula1scope',key='formula1AccountKey')

# COMMAND ----------



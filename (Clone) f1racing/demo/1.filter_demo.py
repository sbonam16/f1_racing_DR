# Databricks notebook source
# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

races_df= spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_filter= races_df.filter('race_year=2009 and round<5')

# COMMAND ----------

races_filter= races_df.filter((races_df['race_year']==2009) & (races_df['round']==5))

# COMMAND ----------

display(races_filter)

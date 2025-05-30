# Databricks notebook source
# MAGIC %md
# MAGIC access dataframe using sql

# COMMAND ----------

# MAGIC %md
# MAGIC objectives
# MAGIC 1) create temp views on dataframe
# MAGIC 2) access view from sql 
# MAGIC 3) access the view from python

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC FROM v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_dataframe = spark.sql("select * from v_race_results where race_year=2020")

# COMMAND ----------

# MAGIC %md
# MAGIC global temp view 
# MAGIC 1. create global temporary view from sql cell
# MAGIC 2. accees the view from sql cell
# MAGIC 3. access the view from python cell
# MAGIC 4. access the view from another notebook 

# COMMAND ----------

 race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABles IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

race_global_df= spark.sql('select * from global_temp.gv_race_results;')

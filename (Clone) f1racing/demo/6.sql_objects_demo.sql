-- Databricks notebook source
-- MAGIC %md
-- MAGIC lesson objectives
-- MAGIC 1. spark sql doc
-- MAGIC 2. creaete database demo 
-- MAGIC 3. data tab in ui
-- MAGIC 4.SHOW command
-- MAGIC 5. Describe command
-- MAGIC 6.find the current database

-- COMMAND ----------

create database IF NOT exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in demo ;


-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %run "../INCLUDES/configuration"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_df=spark.read.parquet(F'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

SHOW DATABASES;

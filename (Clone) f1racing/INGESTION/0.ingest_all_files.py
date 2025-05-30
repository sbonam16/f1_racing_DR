# Databricks notebook source
v_result = dbutils.notebook.run("1.Ingest_circuit_file", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_race_file", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_construction_file", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_driver_file", 0,{'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_data", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("6. ingestion_pitstop_pdf", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_laps_files", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_files", 0, {'p_data_source': "Ergast API", 'p_file_date': "2021-04-18"})

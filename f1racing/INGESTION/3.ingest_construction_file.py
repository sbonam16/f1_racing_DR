# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

const_schema= 'constructorId INT,constructorRef STRING,name STRING , nationality STRING, url STRING'

# COMMAND ----------

const_df = spark.read.schema(const_schema).json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

display(const_df)

# COMMAND ----------

const_column_df= add_ingestion_date(const_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###drop certain columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

const_dropped_df= const_column_df.drop(col('url'))

# COMMAND ----------

const_renamed_df = const_dropped_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed ('constructorRef','constructor_ref').withColumn('data_source',lit(v_data_source)).withColumn('file_name',lit(v_file_date))

# COMMAND ----------

const_renamed_df.write.mode('overwrite').format("delta").saveAsTable('f1_processed.constructors')

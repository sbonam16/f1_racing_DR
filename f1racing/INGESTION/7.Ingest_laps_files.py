# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### importing multiple csv files

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

laptime_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

laptime_df= spark.read.schema(laptime_schema).csv(f'{raw_folder_path}/{v_file_date}/lap_times*')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

laptime_renamed= laptime_df.withColumnRenamed('raceID','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumn('data_source',lit(v_data_source))\
    .withColumn('file_date',lit(v_file_date))


# COMMAND ----------

laptime_ingestion_date=add_ingestion_date(laptime_renamed)

# COMMAND ----------

#overwrite_partition(laptime_ingestion_date,'f1_processed','lap_times','race_id')


# COMMAND ----------

mergeCondition="tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
mergeData(laptime_ingestion_date,'f1_processed','lap_times',f"{processed_folder_path}",mergeCondition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

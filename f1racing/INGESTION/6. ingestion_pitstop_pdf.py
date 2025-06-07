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

from pyspark.sql.types import StructField , StructType , StringType , IntegerType

# COMMAND ----------

pitstop_schema= StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("stop",StringType(),True),
    StructField("lap",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

pitstop_df= spark.read\
    .schema(pitstop_schema)\
    .option('multiline',True)\
    .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pitstop_renamed = pitstop_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn('data_source',lit(v_data_source))\
        .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

pitstop_ingestion_date =add_ingestion_date(pitstop_renamed)

# COMMAND ----------

#overwrite_partition(pitstop_ingestion_date,'f1_processed','pitstops','race_id')

# COMMAND ----------

mergeCondition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"

mergeData(pitstop_ingestion_date,'f1_processed','pitstops',f"{processed_folder_path}",mergeCondition,"race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, count(1) FROM f1_processed.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

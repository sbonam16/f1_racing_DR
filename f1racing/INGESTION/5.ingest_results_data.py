# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType , FloatType 

# COMMAND ----------

results_schema=StructType(fields=[
  StructField("resultId", IntegerType(),False),
  StructField("raceId", IntegerType(),True),
  StructField("driverId", IntegerType(),True),
  StructField("constructorId", IntegerType(),True),
  StructField("number", IntegerType(),True),
  StructField("grid", IntegerType(),True),
  StructField("position", IntegerType(),True),
  StructField("positionText", StringType(),True),
  StructField("positionOrder", IntegerType(),True),        
  StructField("points", FloatType(),True),
  StructField("laps", IntegerType(),True),
  StructField("time", StringType()),
  StructField("milliseconds", IntegerType()),
  StructField("fastestLap", IntegerType()),
  StructField("rank", IntegerType()),
  StructField("fastestLapTime", StringType()),
  StructField("fastestLapSpeed", StringType()),
  StructField("statusId", IntegerType())
])

# COMMAND ----------

results_df= spark.read.schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp ,lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("fastestLap", "faster_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumn('data_source',lit(v_data_source))\
        .withColumn('file_date',lit(v_file_date))
 

# COMMAND ----------

results_ingestion_date = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC dedupe the dataframe (remove duplicates)

# COMMAND ----------

results_dedupe_df=results_ingestion_date.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC method 1 for incfemental load 

# COMMAND ----------

#  for race_id_list in results_ingestion_date.select("race_id").distinct().collect():    
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id={race_id_list.race_id} )")


# COMMAND ----------

# results_ingestion_date.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC method 2 for incremental load
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

#overwrite_partition(results_ingestion_date,'f1_processed','results','race_id')

# COMMAND ----------

mergeCondition="tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
mergeData(results_dedupe_df,'f1_processed','results',f"{processed_folder_path}",mergeCondition,"race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id,count(1) from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %md
# MAGIC to check duplicates

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC select race_id,driver_id,count(1) from f1_processed.results
# MAGIC group by race_id,driver_id
# MAGIC HAVING count(1)>1
# MAGIC order by race_id,driver_id desc

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

# MAGIC %md
# MAGIC ### importing multiple json files

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId", IntegerType(),False),
                                      StructField("raceId", IntegerType(),True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

qualifying_df= spark.read.schema(qualifying_schema).option('multiline',True).json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_renamed= qualifying_df.withColumnRenamed('raceID','race_id')\
    .withColumnRenamed('driverId','driver_id')\
        .withColumnRenamed('constructorId','constructor_id')\
            .withColumnRenamed('qualifyID','qualify_ID')\
                .withColumn('data_source',lit(v_data_source))\
                    .withColumn('file_date',lit(v_file_date))
   

# COMMAND ----------

qualifying_ingestion_date=add_ingestion_date(qualifying_renamed)

# COMMAND ----------

#overwrite_partition(qualifying_ingestion_date,'f1_processed','qualifying','race_id')

# COMMAND ----------

mergeCondition="tgt.qualify_ID = src.qualify_ID AND tgt.race_id = src.race_id"
mergeData(qualifying_ingestion_date,'f1_processed','qualifying',f"{processed_folder_path}",mergeCondition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

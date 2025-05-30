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
# MAGIC read json file using spark dataframe

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField ,IntegerType , DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(),True), 
                                StructField('surname', StringType(),True)])

# COMMAND ----------

drivers_schema= StructType(fields=[StructField('driverId', IntegerType(),True), 
                                   StructField('driverRef', StringType(),True),
                                   StructField('number', IntegerType(),True),
                                   StructField('code', StringType(),True),
                                   StructField('name', name_schema),
                                   StructField('dob', DateType(),True),
                                   StructField('nationality', StringType(),True),
                                   StructField('url', StringType(),True)
                                   ])   

# COMMAND ----------

drivers_df=spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC > rename column , add new column and join forename and surname and drop url

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat ,col

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('driverID','driver_id')\
    .withColumnRenamed('driverRef','driver_ref')\
    .withColumn('ingesetion_date',current_timestamp())\
    .drop('url')\
    .withColumn('name',concat(col("name.forename"), lit(' '), col('name.surname')))\
    .withColumn('data_source',lit(v_data_source))\
    .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

drivers_renamed_df.write.mode('overwrite').format("parquet").saveAsTable('f1_processed.drivers')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/drivers'))

# COMMAND ----------



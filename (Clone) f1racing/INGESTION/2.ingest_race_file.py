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

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType,DateType,TimestampType

# COMMAND ----------

races_schema= StructType(fields=[StructField('raceId',IntegerType(),False),
                                StructField('year',IntegerType(),True),
                                StructField('round',IntegerType(),True),
                                StructField('circuitId',IntegerType(),True),
                                StructField('name',StringType(),True),
                                StructField('date',DateType(),True),
                                StructField('time',StringType(),True),
                                StructField('url',StringType(),True),
                                ])

# COMMAND ----------

races_df=spark.read.option('header',True).schema(races_schema).csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,lit,concat, current_timestamp,col,lit

# COMMAND ----------

races_with_timestamp = races_df.withColumn('ingestionDate', current_timestamp()).withColumn('race_timestamp', 
 to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###remove uncessary rows

# COMMAND ----------

from pyspark.sql.functions import col 


# COMMAND ----------

race_col_df= races_with_timestamp.select(col('raceID'), col('year'), col('round'), col('circuitId'), col('name'), col('ingestionDate') , col('race_timestamp'),col('data_source'),col('file_date')) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename coulmn name 

# COMMAND ----------

races_renamed_df = race_col_df.withColumnRenamed('raceID','race_id') \
                              .withColumnRenamed('year','race_year')\
                              .withColumnRenamed('circuitId','circuit_id')\
                              

# COMMAND ----------

races_renamed_df.write.mode('overwrite').format("parquet").saveAsTable('f1_processed.races')

# COMMAND ----------

display(races_renamed_df)

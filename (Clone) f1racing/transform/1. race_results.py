# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df=spark.read.parquet(f'{processed_folder_path}/races').withColumnRenamed('name','race_name').withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

circuits_df=spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('location','circuit_location')

# COMMAND ----------

drivers_df=spark.read.parquet(f'{processed_folder_path}/drivers').withColumnRenamed('name','driver_name').withColumnRenamed('number','driver_number').withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructor_df=spark.read.parquet(f'{processed_folder_path}/constructors').withColumnRenamed('name','team')

# COMMAND ----------

results_df=spark.read.parquet(f'{processed_folder_path}/results')\
    .filter(f"file_date='{v_file_date}'")\
        .withColumnRenamed('time','race_time')\
        .withColumnRenamed('race_id','results_race_id')\
            .withColumnRenamed('file_date','results_file_date')

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df, races_df.circuit_id==circuits_df.circuit_id, 'inner') 

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df, results_df.results_race_id==race_circuits_df.race_id, 'inner')\
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id, 'inner')\
    .join(constructor_df, results_df.constructor_id==constructor_df.constructor_id, 'inner')\
   


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_Df=race_results_df.select('race_id','race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','race_time','points','position','results_file_date').withColumn('created_date',current_timestamp())\
    .withColumnRenamed('results_file_date','file_date')

# COMMAND ----------

overwrite_partition(final_Df,'f1_presentation','race_results','race_id')

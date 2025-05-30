# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC driver standings
# MAGIC

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date='{v_file_date}'")\
        .select('race_year')\
            .distinct().collect()

# COMMAND ----------

race_year_list=[]
for race in races_results_df:
    race_year_list.append(race[0])


# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import count, sum,when , col

# COMMAND ----------

driver_standing_df = race_results_df\
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank= Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank))

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings 

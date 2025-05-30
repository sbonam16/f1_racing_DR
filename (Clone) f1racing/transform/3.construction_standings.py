# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-28')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../INCLUDES/common_functions"

# COMMAND ----------

# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

constructor_standing_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date='{v_file_date}'").select('race_year').distinct().collect()


# COMMAND ----------

column=[]
for i in constructor_standing_df:
    column.append(i[0])
print(column)

# COMMAND ----------

from pyspark.sql.functions import col
constructor_standing_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(column))

# COMMAND ----------

from pyspark.sql.functions import count, sum,when , col

# COMMAND ----------

constructor_df = constructor_standing_df.groupBy('race_year','team').agg( sum('points').alias('points'),count(when(col('position')==1, True)).alias('wins'),)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
constructor_rank= Window.partitionBy('race_year').orderBy(desc('points'),desc('wins'))
team_rank_df = constructor_df.withColumn('rank', rank().over(constructor_rank))

# COMMAND ----------


overwrite_partition(team_rank_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings

# Databricks notebook source
# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC aggregate functions demo

# COMMAND ----------

# MAGIC %md 
# MAGIC built in aggregate functions

# COMMAND ----------

race_results_df= spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")
display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_year")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"),countDistinct("race_year")).withColumnRenamed("sum(points)","total_points").withColumnRenamed("count(Distinct race_year)","number_of_years").show()

# COMMAND ----------

demo_df.groupBy('driver_name').agg(sum("points"),countDistinct("race_name")).withColumnRenamed("sum(points)","total_points").withColumnRenamed("count(Distinct race_name)","race_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC windoow function
# MAGIC

# COMMAND ----------

demo_df= race_results_df.filter("race_year in(2019,2020)")

# COMMAND ----------

demo_grouped_df=demo_df.groupBy('race_year','driver_name').agg(sum("points").alias('race_points'),countDistinct("race_name").alias('race_name'))

# COMMAND ----------

display(demo_grouped_df)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

driver_rank = Window.partitionBy('race_year').orderBy(desc('race_points'))
demo_grouped_df.withColumn('rank',rank().over(driver_rank)).show(100)

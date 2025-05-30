# Databricks notebook source
# MAGIC %run "../INCLUDES/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f'{processed_folder_path}/circuits').filter('circuit_id < 70').withColumnRenamed('name','circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df=spark.read.parquet(f'{processed_folder_path}/races').filter("race_year=2019").withColumnRenamed('name','race_name')

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'inner').select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#left_outer_JOin
race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'left').select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#right_outer_JOin
race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'right').select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#left_outer_JOin
race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'full').select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi jonts
# MAGIC shows only things on left data frame i.e circuits_df which matches with races_df

# COMMAND ----------

race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'semi')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC anti joins
# MAGIC opposite to semi

# COMMAND ----------

race_circuits_df= circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'anti')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC cross joins

# COMMAND ----------

race_circuits_df=races_df.crossJoin(circuits_df)
display(race_circuits_df)

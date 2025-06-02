# Databricks notebook source
# MAGIC %md
# MAGIC #ingest circuit.csv file

# COMMAND ----------

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

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType 

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),False),
                                   StructField('circuitRef',StringType(),True),
                                   StructField('name',StringType(),True),
                                   StructField('location',StringType(),True),
                                   StructField('country',StringType(),True),
                                   StructField('lat',DoubleType(),True),
                                   StructField('lng',DoubleType(),True),
                                   StructField('alt',IntegerType(),True),
                                   StructField('url',StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.option('header',True).schema(circuits_schema).csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### select only required columns
# MAGIC remove url from column

# COMMAND ----------

#type 1 
# circuits_selected_df = circuits_df.select('circuitId','circuitRef','name','location','country','lat','lng','alt')

# COMMAND ----------

#type-2
from pyspark.sql.functions import col  

# COMMAND ----------

circuits_selected_df=circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###rename the column using as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_rename_df=circuits_selected_df.withColumnRenamed('circuitId','circuit_id').withColumnRenamed('circuitref','circuit_ref').withColumnRenamed('lat','latitude').withColumnRenamed('lng','longitude').withColumnRenamed('alt','altitude')

# COMMAND ----------

circuits=circuits_rename_df.withColumn('data_source',lit(v_data_source)).withColumn('filedate',lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###add ingestion date to dataframe

# COMMAND ----------


circuits_final_df= add_ingestion_date(circuits)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write data to datalake as parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit(f"Parameter received: {v_data_source}")

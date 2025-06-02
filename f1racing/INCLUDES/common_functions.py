# Databricks notebook source
from pyspark.sql.functions import current_timestamp 
def add_ingestion_date (input_df) :
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_col(input_df,parition):
    column=[]
    for rearrange_col in input_df.schema.names:
        if rearrange_col != parition:
            column.append(rearrange_col)
    column.append(parition)
    output_df = input_df.select(column)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df,database_name,table_name,partition):
    output_df=rearrange_col(input_df,partition)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f'{database_name}.{table_name}')):
        output_df.write.mode("overwrite").insertInto(f'{database_name}.{table_name}')
    else :
        output_df.write.mode("overwrite").partitionBy(partition).format("parquet").saveAsTable(f'{database_name}.{table_name}')

# COMMAND ----------


def mergeData(input_df,db_name,table_name,folder_path,merge_condition,partition): 
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")   
    from delta.tables import DeltaTable 
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
            detla_table= DeltaTable.forPath(spark,f'{folder_path}/{table_name}')
            detla_table.alias("tgt").merge(
                input_df.alias("src"),merge_condition
                )\
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                        .execute()

    else :
            input_df.write.mode("overwrite").partitionBy(partition).format("delta").saveAsTable(f"{db_name}.{table_name}")

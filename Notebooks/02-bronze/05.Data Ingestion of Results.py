# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

results_source_file = f"{landing_folder_path}/results"
results_table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read csv file** 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType
# defines schema

results_schema = StructType([
    StructField('constructorId', StringType(), True),
    StructField('date', DateType(), True),
    StructField('driverId', StringType(), True),
    StructField('grid', IntegerType(), True), 
    StructField('laps', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('raceName', StringType(), True),
    StructField('round', IntegerType(), True),
    StructField('season', IntegerType(), True),
    StructField('status', StringType(), True), 
    StructField('url', StringType(), True)  
])


# COMMAND ----------

results_df = (
    spark.read
    .format('json')
    .option('header', True)
    .option('mode', 'FAILFAST')
    .schema(results_schema)
    .load(results_source_file)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Add metadata columns** 

# COMMAND ----------

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to bronze delta table**

# COMMAND ----------

(
    results_final_df
    .write 
    .format('delta')
    .mode('overwrite')
    .saveAsTable(results_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

races_source_file = f"{landing_folder_path}/races.csv"
races_table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read csv file** 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType
# defines schema
races_schema = StructType([
    StructField('season', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('url', StringType(), True),
    StructField('raceName', StringType(), True), 
    StructField('date', DateType(), True),
    StructField('circuitId', StringType(), True)
])

# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header', True)
    .option('mode', 'FAILFAST')
    .schema(races_schema)
    .load(races_source_file)
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

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to bronze delta table**

# COMMAND ----------

(
    races_final_df
    .write 
    .format('delta')
    .mode('overwrite')
    .saveAsTable(races_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
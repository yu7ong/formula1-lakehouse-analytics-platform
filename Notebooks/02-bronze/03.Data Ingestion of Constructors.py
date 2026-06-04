# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

constructors_source_file = f"{landing_folder_path}/constructors.json"
constructors_table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read csv file** 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType
# defines schema
constructors_schema = StructType([
    StructField('constructorId', StringType(), True),
    StructField('name', StringType(), True), 
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

constructors_df = (
    spark.read
    .format('json')
    .option('header', True)
    .option('mode', 'FAILFAST')
    .schema(constructors_schema)
    .load(constructors_source_file)
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

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to bronze delta table**

# COMMAND ----------

(
    constructors_final_df
    .write 
    .format('delta')
    .mode('overwrite')
    .saveAsTable(constructors_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
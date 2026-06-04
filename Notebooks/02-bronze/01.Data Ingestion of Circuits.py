# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config
# MAGIC

# COMMAND ----------

circuits_source_file = f"{landing_folder_path}/circuits.csv"
circuits_table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC **Read csv file** 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType
# defines schema
circuits_schema = StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True), 
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])


# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header', True)
    .option('mode', 'FAILFAST')
    .schema(circuits_schema)
    .load(circuits_source_file)
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


circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to bronze delta table**

# COMMAND ----------

(
    circuits_final_df
    .write 
    .format('delta')
    .mode('overwrite')
    .saveAsTable(circuits_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
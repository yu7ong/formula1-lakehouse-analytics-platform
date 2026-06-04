# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from the delta table

# COMMAND ----------

circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only required column 

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    "circuitId",
    "circuitName",
    "lat",
    "long",
    "locality",
    "country",
    "ingestion_timestamp",
    "source_file"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize column names and rename them into meaningful names

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitName", "circuit_name")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("long", "longitude")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform data quality checks 

# COMMAND ----------

# remove null values from column circuit_id
circuits_valid_df = circuits_renamed_df.filter(
    "circuit_id IS NOT NULL"
)

# COMMAND ----------

# remove duplicated rows
circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC Tranform and standardize values

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
    .withColumn("locality", F.initcap(F.col("locality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer

# COMMAND ----------

(
    circuits_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
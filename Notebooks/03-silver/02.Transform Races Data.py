# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from the delta table

# COMMAND ----------

races_df = spark.read.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only required column 

# COMMAND ----------

races_selected_df = races_df.select(
    "season",
    "round",
    "raceName",
    "date",
    "circuitId",
    "ingestion_timestamp",
    "source_file"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize column names and rename them into meaningful names

# COMMAND ----------

races_renamed_df = (
    races_selected_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("raceName", "race_name")
    .withColumnRenamed("date", "race_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform data quality checks 

# COMMAND ----------

# remove duplicated rows
races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

# MAGIC %md
# MAGIC Tranform and standardize values

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn("race_name", F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer

# COMMAND ----------

(
    races_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
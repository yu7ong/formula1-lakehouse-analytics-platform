# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from the delta table

# COMMAND ----------

constructors_df = spark.read.table(bronze_table)
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only required column 

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize column names and rename them into meaningful names

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("name", "constructor_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform data quality checks 

# COMMAND ----------

# remove duplicated rows
constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC Tranform and standardize values

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn("nationality", F.initcap(F.col("nationality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer

# COMMAND ----------

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from the delta table

# COMMAND ----------

drivers_df = spark.read.table(bronze_table)
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only required column 

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize column names and rename them into meaningful names

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("dateOfBirth", "date_of_birth")
)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

drivers_concatenated_df = (
    drivers_renamed_df
    .withColumn("driver_name", F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
    .drop("name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform data quality checks 

# COMMAND ----------

# remove duplicated rows
drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC Tranform and standardize values

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn("nationality", F.initcap(F.col("nationality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer

# COMMAND ----------

(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
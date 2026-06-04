# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from the delta table

# COMMAND ----------

sprints_df = spark.read.table(bronze_table)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Keep only required column 

# COMMAND ----------

sprints_dropped_df = sprints_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Standardize column names and rename them into meaningful names

# COMMAND ----------

sprints_renamed_df = (
    sprints_dropped_df
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("raceName", "race_name")
    .withColumnRenamed("date", "race_date")
    .withColumnRenamed("grid", "grid_position")
    .withColumnRenamed("laps", "completed_laps")
    .withColumnRenamed("number", "car_number")
    .withColumnRenamed("position", "final_position")
    .withColumnRenamed("positionText", "final_position_text")
)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Perform data quality checks 

# COMMAND ----------

# remove null values 
sprints_valid_df = (
    sprints_renamed_df
    .filter(
        F.col("season").isNotNull() & 
        F.col("round").isNotNull() &
        F.col("driver_id").isNotNull() &
        F.col("constructor_id").isNotNull()
    )
)

# COMMAND ----------

display(sprints_renamed_df.count() - sprints_valid_df.count())

# COMMAND ----------

# remove duplicated rows
sprints_distinct_df = sprints_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

display(sprints_valid_df.count() - sprints_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Tranform and standardize values

# COMMAND ----------

sprints_final_df = (
    sprints_distinct_df
    .withColumn("race_name", F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write transformed data to silver layer

# COMMAND ----------

(
    sprints_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
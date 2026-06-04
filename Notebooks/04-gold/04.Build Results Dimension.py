# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results" 

# COMMAND ----------

# read source tables 
results_df = (
    spark.read.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn("sessionType", F.lit("RACE"))
    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
    )
sprints_df = (
    spark.read.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("sessionType", F.lit("SPRINT"))
    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
    )


# COMMAND ----------

# Combine data frames
results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# Add derived columns 
fact_session_results_df = (
    results_sprints_df
    .withColumn("is_win", (F.col("final_position") == 1))
    .withColumn("is_podium", F.col("final_position").between(1,3))
    .withColumn("has_points", (F.col("points") > 0))
)


# COMMAND ----------

(
    fact_session_results_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)
# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers" 

# COMMAND ----------

# read source tables 
drivers_df = spark.read.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.read.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# Join table anbd select relevant columns
dim_drivers_df = (
    drivers_df
    .join(ref_nationality_region_df, 
                  drivers_df.nationality == ref_nationality_region_df.nationality, 
                  "left_outer")
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

(
    dim_drivers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)
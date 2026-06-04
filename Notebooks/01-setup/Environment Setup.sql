-- Databricks notebook source
-- MAGIC %md
-- MAGIC create external location to datasets stored in azure data lake storage

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `databricks_project_1_formula1`
URL 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-project-1-cred`)
COMMENT 'External location for databricks-project-1';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1
   MANAGED LOCATION 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/'
   COMMENT 'Main catalog';

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing; 
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/gold';

-- COMMAND ----------

USE CATALOG formula1; 
SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create volume files

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://formula1@yu7ongproj1.dfs.core.windows.net/landing'; 


-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files
# Databricks notebook source
# MAGIC %md ### Exploring delta structure
# MAGIC
# MAGIC Delta is composed of parquet files and a transactional log

# COMMAND ----------

files = dbutils.fs.ls("/mnt/data/bronze/citi_bike_data")

display(files)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/data/bronze/citi_bike_data/_delta_log/")

display(files)

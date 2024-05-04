# Databricks notebook source
# MAGIC %md
# MAGIC ### Let's take a look at our data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike.fact_trips limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Now let's look at the history against the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY citi_bike.fact_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can now look at specific versions as of a set timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike.fact_trips timestamp as of '2023-11-20'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Or we can look at it at a set version

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike.fact_trips version as of  3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE citi_bike.fact_trips_20240210 CLONE citi_bike.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table citi_bike.fact_trips_20240210

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast joins

# COMMAND ----------

from pyspark.sql.functions import broadcast
import time

# Create a large DataFrame with 'id' as the key
data_large = [(i, f"Value_{i}") for i in range(1, 1000001)]
columns_large = ["id", "value"]
large_df = spark.createDataFrame(data_large, columns_large)

# Create a small DataFrame with 'unique_id' as the key
data_small = [(i, f"Feature_{i}") for i in range(1, 101)]
columns_small = ["unique_id", "feature"]
small_df = spark.createDataFrame(data_small, columns_small)

# Regular join with explicit join conditions
start_time_reg = time.time()
regular_join_df = large_df.join(small_df, large_df.id == small_df.unique_id)
regular_join_df.count()  # Action to trigger computation
time_reg_join = time.time() - start_time_reg

# Broadcast join with explicit join conditions
start_time_broad = time.time()
broadcast_join_df = large_df.join(broadcast(small_df), large_df.id == small_df.unique_id)
broadcast_join_df.count()  # Action to trigger computation
time_broad_join = time.time() - start_time_broad

# Print performance results
print(f"Regular Join Time: {time_reg_join:.2f} seconds")
print(f"Broadcast Join Time: {time_broad_join:.2f} seconds")

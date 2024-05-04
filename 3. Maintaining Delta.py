# Databricks notebook source
# MAGIC %md
# MAGIC ### Optimizing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Drop table citi_bike_dev.bronze.customers;
# MAGIC Drop table citi_bike_dev.bronze.fact_trips_Zorder

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE citi_bike_dev.bronze.customers
# MAGIC (
# MAGIC   Customer_ID INT,
# MAGIC   Customer_Name VARCHAR(50)
# MAGIC )

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (1, 'Josh');
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (2, 'James');
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (3, 'Matt');

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (4, 'Chris');
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (5, 'Todd');
# MAGIC Insert into citi_bike_dev.bronze.customers
# MAGIC (Customer_ID, Customer_Name)
# MAGIC Values
# MAGIC (6, 'Jeff');

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM citi_bike_dev.bronze.customers where Customer_Name = 'Jeff'

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC describe history citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC optimize citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vacuum

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC vacuum citi_bike_dev.bronze.customers RETAIN 0 hours dry run;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC vacuum citi_bike_dev.bronze.customers RETAIN 0 hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history citi_bike_dev.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from citi_bike_dev.bronze.customers version as of 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Z Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE citi_bike_dev.bronze.customers COMPUTE STATISTICS for columns customer_id;
# MAGIC
# MAGIC DESC EXTENDED citi_bike_dev.bronze.customers customer_id

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE citi_bike_dev.bronze.fact_trips_Zorder CLONE citi_bike_dev.silver.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC optimize citi_bike_dev.bronze.fact_trips_Zorder
# MAGIC zorder by (Bike_Key)

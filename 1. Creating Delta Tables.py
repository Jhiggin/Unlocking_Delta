# Databricks notebook source
# MAGIC %md
# MAGIC # Demo - Introduction to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a mount point to our datalake to make accessing data easier

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://data@codesqldl.blob.core.windows.net",
    mount_point="/mnt/data",
    extra_configs={
        "fs.azure.account.key.codesqldl.blob.core.windows.net": dbutils.secrets.get(
            scope="datalake_scope", key="dlkey"
        )
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract data from Data lake to Spark Dataframe

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

# set the data lake file location:
file_location = (
    "/mnt/data/in/*.csv"
)

# Define schema
schema = StructType(
    [
        StructField("trip_duration", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("stop_time", TimestampType(), True),
        StructField("start_station_id", IntegerType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_latitude", DoubleType(), True),
        StructField("start_station_longitude", DoubleType(), True),
        StructField("end_station_id", IntegerType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_latitude", DoubleType(), True),
        StructField("end_station_longitude", DoubleType(), True),
        StructField("bike_id", IntegerType(), True),
        StructField("user_type", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("gender", IntegerType(), True),
    ]
)

# load the data from the csv file to a data frame
df_citi_bike_data = (
    spark.read.option("header", "true")
    .schema(schema)
    .option("delimiter", ",")
    .csv(file_location)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a date field off of start_time to partition by later

# COMMAND ----------

from pyspark.sql.functions import to_date

df_citi_bike_data = df_citi_bike_data.withColumn('Rental_Date', to_date('start_time'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the dataframe to review what data was pulled

# COMMAND ----------

display(df_citi_bike_data)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, year, month, dayofmonth, quarter, dayofweek
from datetime import datetime, timedelta

def create_date_table(start_date, end_date):
    """
    Create a date dimension table.
    
    :param start_date: string, start date in 'YYYY-MM-DD' format
    :param end_date: string, end date in 'YYYY-MM-DD' format
    :return: DataFrame of the date dimension
    """
    # Create a Spark session
    spark = SparkSession.builder.appName("Create Date Table").getOrCreate()

    # Generate a range of dates
    total_days = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days
    date_list = [(datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=x)).date() for x in range(total_days + 1)]
    dates_df = spark.createDataFrame(date_list, 'date')

    # Expand the date information
    date_table = dates_df.select(
        col('value').alias('date'),
        year('date').alias('year'),
        quarter('date').alias('quarter'),
        month('date').alias('month'),
        dayofmonth('date').alias('day_of_month'),
        dayofweek('date').alias('day_of_week'),
        date_format('date', 'E').alias('weekday'),
        (dayofweek('date') > 5).cast('boolean').alias('is_weekend')
    )

    return date_table

# Example usage of the function
start_date = '2005-01-01'
end_date = '2030-12-31'
date_table_df = create_date_table(start_date, end_date)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading data to Delta

# COMMAND ----------

bronze_delta_location = "citi_bike_dev.bronze"

df_citi_bike_data.write.format("delta").mode("overwrite").saveAsTable(
    bronze_delta_location + ".citi_bike_data"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike_dev.bronze.citi_bike_data limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe detail citi_bike_dev.bronze.citi_bike_data

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower
from pyspark.sql.functions import monotonically_increasing_id

df_start_stations = spark.sql(
    "SELECT start_station_id as Station_ID, start_station_name as Station_Name, start_station_latitude as Station_Latitude, start_station_longitude as Station_Longitude FROM citi_bike_dev.bronze.citi_bike_data"
)

df_end_stations = spark.sql(
    "SELECT end_station_id as Station_ID, end_station_name as Station_Name, end_station_latitude as Station_Latitude, end_station_longitude as Station_Longitude FROM citi_bike_dev.bronze.citi_bike_data"
)

df_stations = df_start_stations.union(df_end_stations)

df_stations = df_stations.distinct()

df_bikes = spark.sql("SELECT bike_id from citi_bike_dev.bronze.citi_bike_data")

df_bikes = df_bikes.distinct()

# Add an key column to each dataframe to act as a Surrogate Key.
df_stations = df_stations.withColumn("Stations_key", monotonically_increasing_id())
df_bikes = df_bikes.withColumn("Bike_key", monotonically_increasing_id())

# COMMAND ----------

df_stations.groupBy(df_stations.columns).count().filter("count > 1").show(
    truncate=False
)

df_bikes.groupBy(df_bikes.columns).count().filter("count > 1").show(truncate=False)

# COMMAND ----------

# Writing data to persistent storage (for example, Delta Lake)
silver_location = "citi_bike_dev.silver"

df_stations.write.format("delta").mode("overwrite").saveAsTable(
    silver_location + ".dim_stations"
)

df_bikes.write.format("delta").mode("overwrite").saveAsTable(
    silver_location + ".dim_bikes"
)

date_table_df.write.format("delta").mode("overwrite").saveAsTable(
    silver_location + ".dim_date"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike_dev.silver.dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from citi_bike.dim_stations where Station_ID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   station_id,
# MAGIC   COUNT(*) AS count
# MAGIC FROM
# MAGIC   citi_bike_dev.silver.dim_stations
# MAGIC GROUP BY
# MAGIC   station_id
# MAGIC HAVING
# MAGIC   COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Stations_key,
# MAGIC   Station_id,
# MAGIC   Station_name,
# MAGIC   Station_latitude,
# MAGIC   Station_longitude
# MAGIC FROM
# MAGIC   citi_bike_dev.silver.dim_stations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   citi_bike_dev.silver.dim_bikes

# COMMAND ----------

df_trips = spark.sql(" \
     SELECT  isnull(d.date, -1) as Rental_Date_Key, bd.trip_duration, isnull(s.stations_key, -1) as Start_Station_Key, isnull(es.stations_key, -1) as End_Station_Key, isnull(b.bike_key, -1) as Bike_Key  \
     FROM citi_bike_dev.bronze.citi_bike_data as bd \
     JOIN citi_bike_dev.silver.dim_date d ON bd.rental_date = d.Date \
     LEFT JOIN citi_bike_dev.silver.dim_bikes b ON bd.bike_id = b.bike_id \
     LEFT JOIN citi_bike_dev.silver.dim_stations s on bd.start_station_id = s.station_id \
     LEFT JOIN citi_bike_dev.silver.dim_stations es on bd.end_station_id = es.station_id \
")

# COMMAND ----------

display(df_trips)

# COMMAND ----------

display(df_trips)

# COMMAND ----------

df_trips.write.format("delta").mode("overwrite").saveAsTable(
    silver_location + ".fact_trips"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from citi_bike_dev.silver.fact_trips

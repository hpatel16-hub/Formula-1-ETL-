# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest multiple CSV files

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the csv file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

laptime_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(),True),          # Only to be used when the data is too big in production
                                    StructField("position", IntegerType(),True),           # So, inferschema does take less time to identify all of data
                                    StructField("time", StringType(),True),           # For small data use inferschema = True or False while reading
                                    StructField("milliseconds", IntegerType(),True)        
])

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.charVarcharAsString", "true") # char/varchar type can only be used in the table schema

# COMMAND ----------

laptimes_df = spark.read.schema(laptime_schema).csv("/mnt/formula1racing/raw-files/lap_times")
display(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and add column

# COMMAND ----------

laptimes_renamed_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("ingestion_date", current_timestamp())           
display(laptimes_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

laptimes_renamed_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/lap_times")

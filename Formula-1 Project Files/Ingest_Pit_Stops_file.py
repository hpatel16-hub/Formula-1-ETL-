# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(),True),           # Only to be used when the data is too big in production
                                    StructField("lap", IntegerType(),True),           # So, inferschema does take less time to identify all of data
                                    StructField("time", StringType(),True),           # For small data use inferschema = True or False while reading
                                    StructField("duration", StringType(),True),
                                    StructField("milliseconds", IntegerType(),True)        
])

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.charVarcharAsString", "true") # char/varchar type can only be used in the table schema

# COMMAND ----------

pitstops_df = spark.read.schema(pitstop_schema).option("multiLine", True).json("/mnt/formula1racing/raw-files/pit_stops.json")
display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and add column

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("ingestion_date", current_timestamp())           
display(pitstops_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

pitstops_renamed_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/pit_stops")

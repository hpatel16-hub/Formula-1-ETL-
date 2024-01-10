# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType, FloatType, VarcharType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True), # Only to be used when the data is too big in production
                                    StructField("driverId", IntegerType(), True),   # So, inferschema does take less time to identify all of data  
                                    StructField("constructorId", IntegerType(), True),      # For small data use inferschema = True or False while reading 
                                    StructField("number", IntegerType(), ),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(),True),
                                    StructField("positionText", VarcharType(255),True),
                                    StructField("positionOrder", IntegerType(),True),
                                    StructField("points", FloatType(),True),
                                    StructField("laps", IntegerType(),True),
                                    StructField("time", VarcharType(255),True),
                                    StructField("milliseconds", IntegerType(),True),
                                    StructField("fastestLap", IntegerType(),True),
                                    StructField("rank", IntegerType(),True),
                                    StructField("fastestLapTime", VarcharType(255),True),
                                    StructField("FastestLapSpeed", VarcharType(255),True),
                                    StructField("statusId", IntegerType(),True)
                                    
])

# COMMAND ----------

spark.conf.set("spark.sql.legacy.charVarcharAsString", "true") # char/varchar type can only be used in the table schema

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/formula1racing/raw-files/results.json")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and add column

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("positionText", "position_text") \
                               .withColumnRenamed("positionOrder", "position_order") \
                               .withColumnRenamed("fastestLap", "fastest_lap") \
                               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                               .withColumnRenamed("FastestLapSpeed", "fastest_lap_speed") \
                               .withColumn("ingestion_date", current_timestamp()) 
                                      
display(results_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

results_dropped_df = results_renamed_df.drop(col('statusId'))
# constructors_dropped_df = constructors_df.drop(constructors_df['url']) 
# Another way for dropping columns
display(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

results_dropped_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1racing/processed/results")

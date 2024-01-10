# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, concat, lit

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                        StructField("driverRef", StringType(), True), # Only to be used when the data is too big in production
                                        StructField("number", IntegerType(), True),   # So, inferschema does take less time to identify all of data  
                                        StructField("code", StringType(), True),      # For small data use inferschema = True or False while reading 
                                        StructField("name", name_schema),
                                        StructField("dob", DateType(), True),
                                        StructField("nationality", StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/formula1racing/raw-files/drivers.json")
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and add column

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("driverRef", "driver_ref") \
                               .withColumn("ingestion_date", current_timestamp()) \
                               .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname")))          
display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

drivers_dropped_df = drivers_renamed_df.drop(col('url'))
# constructors_dropped_df = constructors_df.drop(constructors_df['url']) 
# Another way for dropping columns
display(drivers_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

drivers_dropped_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/drivers")

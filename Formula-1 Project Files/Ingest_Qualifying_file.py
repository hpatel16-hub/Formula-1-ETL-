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

qualify_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(),True),
                                    StructField("number", IntegerType(),True),          # Only to be used when the data is too big in production
                                    StructField("position", IntegerType(),True),           # So, inferschema does take less time to identify all of data
                                    StructField("q1", StringType(),True),           # For small data use inferschema = True or False while reading
                                    StructField("q2", StringType(),True),
                                    StructField("q3", StringType(),True)        
])

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.charVarcharAsString", "true") # char/varchar type can only be used in the table schema

# COMMAND ----------

qualify_df = spark.read.schema(qualify_schema).option("multiLine", True).json("/mnt/formula1racing/raw-files/qualifying")
display(qualify_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and add column

# COMMAND ----------

qualify_renamed_df = qualify_df.withColumnRenamed("qualifyId", "qualify_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumn("ingestion_date", current_timestamp())           
display(qualify_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

qualify_renamed_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/qualifying")

# Databricks notebook source
# MAGIC %md 
# MAGIC ## read the csv file using spark DataFrame reader
# MAGIC

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),     # Only to be used when the data is too big in production
                                    StructField("lat", DoubleType(), True),         # So, inferschema does take less time identiy all of data fields
                                    StructField("lng", DoubleType(), True),         # For small data use inferschema = True or False 
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(),True),
]) 

# COMMAND ----------

circuit_df = spark.read.schema(circuit_schema).csv("/mnt/formula1racing/raw-files/circuits.csv", header= True)
display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select only require columns

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), 
                                        col("country"), col("lat"), col("lng"), col("alt")    
)

display(circuit_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the columns as required 

# COMMAND ----------

# This is also another method to replace column names 
# circuit_selected_df = circuit_df.select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"), col("location"), 
#                                        col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude")    
# )  

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude")

display(circuit_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add ingestion date to dataframe 

# COMMAND ----------

circuits_final_df = circuit_renamed_df.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the final dataframe into Parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/circuits")
display(circuits_final_df)

# Databricks notebook source
# MAGIC %md 
# MAGIC ## read the Json file using spark DataFrame reader
# MAGIC

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining DataSchema

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                StructField("year", IntegerType(), True),
                                StructField("round", IntegerType(), True),
                                StructField("circuitId", IntegerType(), True),
                                StructField("name", StringType(), True),         # Only to be used when the data is too big in production
                                StructField("date", DateType(), True),           # So, inferschema does take less time to identify all of data 
                                                                                 # fields
                                StructField("time", StringType(), True),         # For small data use inferschema = True or False while reading file
                                StructField("url", StringType(),True)
]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Races csv file

# COMMAND ----------

races_df = spark.read.schema(races_schema).csv("/mnt/formula1racing/raw-files/races.csv", header= True)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the columns as required 

# COMMAND ----------

# This is also another method to replace column names 
# circuit_selected_df = circuit_df.select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"), col("location"), 
#                                        col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude")    
# )  

races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
                           .withColumnRenamed("year", "race_year") \
                           .withColumnRenamed("circuitId", "circuit_id") 

display(races_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add ingestion date to dataframe 

# COMMAND ----------

races_date_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_time", current_timestamp())

display(races_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select only require columns

# COMMAND ----------

races_final_df = races_date_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), 
                                        col("name"), col("race_timestamp"), col("ingestion_time")   
)

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the final dataframe into Parquet (PartitionBy) 
# MAGIC

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/formula1racing/processed/races")
display(races_final_df)

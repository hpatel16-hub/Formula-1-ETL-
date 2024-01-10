# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the json file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import  StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                        StructField("constructorRef", StringType(), True), # Only to be used when the data is too big in production
                                        StructField("name", StringType(), True),           # So, inferschema does take less time to identify all of data 
                                        StructField("nationality", StringType(),True),     # For small data use inferschema = True or False while reading 
                                        StructField("url", StringType(),True)
])
                                            
# constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING       
# Another way for writing schema                                                                                 
                                                                                        
                                

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/formula1racing/raw-files/constructors.json")
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col('url'))
# constructors_dropped_df = constructors_df.drop(constructors_df['url']) 
# Another way for dropping columns
display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename columns and add ingestion date 

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                 .withColumnRenamed("constructorRef", "constructor_ref") \
                                                 .withColumn("ingestion_date", current_timestamp())
display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output file in parquet

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1racing/processed/constructors")

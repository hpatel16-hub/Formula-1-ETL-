# Databricks notebook source
v_result = dbutils.notebook.run("Ingest_circuits_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Constructors_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_drivers_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Lap_times_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Pit_Stops_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Qualifying_file", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Races_files", 0, {"p_source_code" : "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("Ingest_Results_file", 0, {"p_source_code" : "Ergast API"})
v_result

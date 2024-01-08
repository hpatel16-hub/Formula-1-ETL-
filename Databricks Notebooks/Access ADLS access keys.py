# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing ADLS using access keys

# COMMAND ----------

formula1_ADLS = dbutils.secrets.get(scope='formula1-scope', key='formula1racing-ADLS')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1racing.dfs.core.windows.net",
    formula1_ADLS
    )

# COMMAND ----------

display(dbutils.fs.ls("abfs://data-files@formula1racing.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://data-files@formula1racing.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------



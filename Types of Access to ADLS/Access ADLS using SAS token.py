# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing ADLS using SAS Token

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

SAS_Token = dbutils.secrets.get(scope='formula1-scope', key='SAS-Token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1racing.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1racing.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1racing.dfs.core.windows.net", SAS_Token)

# COMMAND ----------

display(dbutils.fs.ls("abfs://data-files@formula1racing.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://data-files@formula1racing.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------



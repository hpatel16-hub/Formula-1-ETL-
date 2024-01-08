# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing ADLS using Service Principal
# MAGIC 1. Register App Registration 
# MAGIC 2. generate secret/password for the application
# MAGIC 3. set spark config with app/ client ID, Directory/ Tenant ID
# MAGIC 4. Assign role 'storage blob data contributor' to the data lake
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

client_Id = dbutils.secrets.get(scope='formula1-scope', key='ClientID')
tenant_Id = dbutils.secrets.get(scope='formula1-scope', key='TenantID')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='SecretKey')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1racing.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1racing.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1racing.dfs.core.windows.net", client_Id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1racing.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1racing.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_Id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfs://data-files@formula1racing.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://data-files@formula1racing.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------



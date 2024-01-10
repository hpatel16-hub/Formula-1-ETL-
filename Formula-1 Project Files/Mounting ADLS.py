# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS Container for project
# MAGIC 1. Get ClientID, TenantID, SecretValue from azure key vaults
# MAGIC 2. set spark config with app/ client ID, Directory/ Tenant ID
# MAGIC 3. call file system utility mount to mount storage file
# MAGIC 4. Explore other file system utilities related to mount( list all mounts, unmount)
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secrets from key vaults
    ClientID = dbutils.secrets.get(scope='formula1-scope', key='ClientID')
    TenantID = dbutils.secrets.get(scope='formula1-scope', key='TenantID')
    SecretKey = dbutils.secrets.get(scope='formula1-scope', key='SecretKey')

    # set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": ClientID,
              "fs.azure.account.oauth2.client.secret": SecretKey,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TenantID}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
              
    # mount storage account  container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net", # container@storage account name
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs) 

    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Raw-files container

# COMMAND ----------

mount_adls('formula1racing', 'raw-files')

# COMMAND ----------

mount_adls('formula1racing', 'processed')

# COMMAND ----------

mount_adls('formula1racing', 'presentation')

# COMMAND ----------

display(dbutils.fs.ls("abfs://data-files@formula1racing.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfs://data-files@formula1racing.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------



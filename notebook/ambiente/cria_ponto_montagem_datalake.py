# Databricks notebook source
# MAGIC %md
# MAGIC * https://adb-7876059274427017.17.azuredatabricks.net/?o=7876059274427017#secrets/createScope

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="escopo-kv-tcc-prd",key="clientid-app-acesso-st-prd"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="escopo-kv-tcc-prd",key="clientsecret-app-acesso-st-prd"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + dbutils.secrets.get(scope="escopo-kv-tcc-prd",key="tenantid-app-acesso-st-prd") + "/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@sttccprd.dfs.core.windows.net",
  mount_point = "/mnt/raw/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/raw/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://delta@sttccprd.dfs.core.windows.net",
  mount_point = "/mnt/delta/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/delta/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://delta@sttccprd.dfs.core.windows.net/bronze",
  mount_point = "/mnt/bronze/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/bronze/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://delta@sttccprd.dfs.core.windows.net/silver",
  mount_point = "/mnt/silver/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/silver/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://delta@sttccprd.dfs.core.windows.net/gold",
  mount_point = "/mnt/gold/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/gold/")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://fs-marchand@adlsmarchand.dfs.core.windows.net",
  mount_point = "/mnt/fs-marchand/",
  extra_configs = configs
)
dbutils.fs.ls("/mnt/fs-marchand/")

# COMMAND ----------

def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)
    else:
        return f"Não existe essa montagem: {str_path}"

# COMMAND ----------

# sub_unmount("/mnt/raw/")

# Databricks notebook source
# MAGIC %md
# MAGIC * https://adb-6201023045706957.17.azuredatabricks.net/?o=6201023045706957#secrets/createScope

# COMMAND ----------

# MAGIC %md
# MAGIC * Client ID: 25d94d9b-1266-40fb-a4d6-78dc01642ca8
# MAGIC * Client Secret: G4x7Q~sNXlqVAQ.-Tbig~XXSyeFnMqJ9msZuR
# MAGIC * Directory (tenant) ID: 6c115cc4-d107-4635-81cd-0617e0344073

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

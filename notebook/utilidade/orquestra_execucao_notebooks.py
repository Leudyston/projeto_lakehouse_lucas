# Databricks notebook source
from multiprocessing.pool import ThreadPool
pool = ThreadPool(10)

# COMMAND ----------

pool.map(
  lambda invalue: dbutils.notebook.run(
    timeout_seconds=60, "/Workspace/Repos/projeto/projeto_lakehouse_stackoverflow_tcc_si/notebook/delta/bronze/stackoverflow", arguments= {"argument": invalue}),
    ["badges", "tags", "users"]
)

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

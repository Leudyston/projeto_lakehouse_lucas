# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Users.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/users'
esquema_users = StructType([
    StructField("_Id", LongType(), True),
    StructField("_Reputation", IntegerType(), True),
    StructField("_CreationDate", TimestampType(), True),
    StructField("_DisplayName", StringType(), True),
    StructField("_LastAccessDate", StringType(), True),
    StructField("_Views", IntegerType(), True),
    StructField("_UpVotes", IntegerType(), True),
    StructField("_DownVotes", IntegerType(), True),
    StructField("_ProfileImageUrl", StringType(), True),
    StructField("_AccountId", LongType(), True)
    
])

# COMMAND ----------

df_users = spark.read.format("xml").options(rootTag="users", rowTag="row").load(dir_origem, schema = esquema_users)

# COMMAND ----------

print(df_users.count())
# arquivo menor =  rows
# arquivo maior = 17.053.422 rows
display(df_users)

# COMMAND ----------

df_users_with_dataImport = df_users.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_users_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "users",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

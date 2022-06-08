# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, from_utc_timestamp, current_timestamp

# COMMAND ----------

param = {
    "dir_origem": "/mnt/raw/processing/stackoverflow/VoteTypes.csv",
    "dir_destino": "/mnt/delta/bronze/stackoverflow/vote_types",
    "esquema_origem": StructType([
        StructField("Id", LongType(), True),
        StructField("Name", StringType(), True)
    ]),
    "tabela_destino": "vote_types",
    "esquema_destino": "bronze"
}

# COMMAND ----------

df = spark.read.format("csv").options(header='True', delimiter=',').load(param['dir_origem'], schema = param['esquema_origem'])

# COMMAND ----------

print(df.count())
display(df)

# COMMAND ----------

df_com_data_controle = df.withColumn('DT_INSERCAO_BRONZE', date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

df_com_data_controle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(param['dir_destino'])

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS {esquema_destino}.{tabela_destino}".format(**param))
spark.sql("CREATE TABLE {esquema_destino}.{tabela_destino} USING DELTA LOCATION '{dir_destino}'".format(**param))
display(spark.sql("SELECT * FROM {esquema_destino}.{tabela_destino} LIMIT 100".format(**param)))

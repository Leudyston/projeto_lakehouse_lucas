# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, from_utc_timestamp, current_timestamp

# COMMAND ----------

param = {
    "dir_origem": "/mnt/raw/processing/stackoverflow/PostHistory.xml",
    "dir_destino": "/mnt/delta/bronze/stackoverflow/post_history",
    "esquema_origem": StructType([
        StructField("_Id", LongType(), True),
        StructField("_PostHistoryTypeId", LongType(), True),
        StructField("_PostId", LongType(), True),
        StructField("_RevisionGUID", StringType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_UserId", LongType(), True),
        StructField("_Text", StringType(), True),
        StructField("_ContentLicense", StringType(), True)
    ]),
    "root_tag": "post_history",
    "tabela_destino": "post_history",
    "esquema_destino": "bronze"
}

# COMMAND ----------

df = spark.read.format("xml").options(rootTag=param['root_tag'], rowTag="row").load(param['dir_origem'], schema = param['esquema_origem'])

# COMMAND ----------

# print(df.count())
# display(df)

# COMMAND ----------

df_com_data_controle = df.withColumn('DT_INSERCAO_BRONZE', date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

df_com_data_controle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(param['dir_destino'])

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS {esquema_destino}.{tabela_destino}".format(**param))
spark.sql("CREATE TABLE {esquema_destino}.{tabela_destino} USING DELTA LOCATION '{dir_destino}'".format(**param))
display(spark.sql("SELECT * FROM {esquema_destino}.{tabela_destino} LIMIT 100".format(**param)))

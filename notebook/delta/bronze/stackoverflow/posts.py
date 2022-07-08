# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, from_utc_timestamp, current_timestamp

# COMMAND ----------

param = {
    "dir_origem": "/mnt/raw/stackoverflow/Posts.xml",
    "dir_destino": "/mnt/delta/bronze/stackoverflow/posts",
    "esquema_origem": StructType([
        StructField("_Id", LongType(), True),
        StructField("_PostTypeId", LongType(), True),
        StructField("_AcceptedAnswerId", LongType(), True),
        StructField("_CreationDate", TimestampType(), True),
        StructField("_Score", IntegerType(), True),
        StructField("_ViewCount", IntegerType(), True),
        StructField("_Body", StringType(), True),
        StructField("_OwnerUserId", LongType(), True),
        StructField("_LastEditorUserId", LongType(), True),
        StructField("_LastEditorDisplayName", StringType(), True),
        StructField("_LastEditDate", TimestampType(), True),
        StructField("_LastActivityDate", TimestampType(), True),
        StructField("_Title", StringType(), True),
        StructField("_Tags", StringType(), True),
        StructField("_AnswerCount", IntegerType(), True),
        StructField("_CommentCount", IntegerType(), True),
        StructField("_FavoriteCount", IntegerType(), True),
        StructField("_ContentLicense", StringType(), True)
    ]),
    "root_tag": "posts",
    "tabela_destino": "posts",
    "esquema_destino": "bronze"
}

# COMMAND ----------

df = spark.read.format("xml").options(rootTag=param['root_tag'], rowTag="row").load(param['dir_origem'], schema = param['esquema_origem'])

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

# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Posts.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/posts'
esquema_posts = StructType([
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
    StructField("_ContentLicense", StringType(), True),
])

# COMMAND ----------

df_posts = spark.read.format("xml").options(rootTag="posts", rowTag="row").load(dir_origem, schema = esquema_posts)

# COMMAND ----------

print(df_posts.count())
# arquivo menor =  rows
# arquivo maior = 55.513.868 rows
display(df_posts)

# COMMAND ----------

df_posts_with_dataImport = df_posts.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_posts_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "posts",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

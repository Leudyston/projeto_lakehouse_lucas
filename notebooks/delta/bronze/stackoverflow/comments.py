# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Comments.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/comments'
esquema_comments = StructType([
    StructField("_Id", LongType(), True),
    StructField("_PostId", LongType(), True),
    StructField("_Score", IntegerType(), True),
    StructField("_Text", StringType(), True),
    StructField("_CreationDate", TimestampType(), True),
    StructField("_UserId", LongType(), True),
    StructField("_ContentLicense", StringType(), True)
])

# COMMAND ----------

df_comments = spark.read.format("xml").options(rootTag="comments", rowTag="row").load(dir_origem, schema = esquema_comments)

# COMMAND ----------

print(df_comments.count())
# arquivo menor =   rows
# arquivo maior = 43.726.638 rows

display(df_comments)

# COMMAND ----------

df_comments_with_dataImport = df_comments.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_comments_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "comments",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
# spark.sql("REFRESH TABLE {esquema}.{tabela}".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

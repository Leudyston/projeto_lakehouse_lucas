# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/PostHistory.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/post_history'
esquema_post_history = StructType([
    StructField("_Id", LongType(), True),
    StructField("_PostHistoryTypeId", LongType(), True),
    StructField("_PostId", LongType(), True),
    StructField("_RevisionGUID", StringType(), True),
    StructField("_CreationDate", TimestampType(), True),
    StructField("_UserId", LongType(), True),
    StructField("_Text", StringType(), True),
    StructField("_ContentLicense", StringType(), True)
])

# COMMAND ----------

df_post_history = spark.read.format("xml").options(rootTag="post_history", rowTag="row").load(dir_origem, schema = esquema_post_history)

# COMMAND ----------

print(df_post_history.count())
# arquivo menor =  rows
# arquivo maior = 147.880.455 rows
display(df_post_history)

# COMMAND ----------

df_post_history_with_dataImport = df_post_history.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_post_history_with_dataImport.write.format("delta").mode("overwrite").partitionBy('DATA_IMPORT').save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "post_history",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

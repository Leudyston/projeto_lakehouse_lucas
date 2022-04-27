# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/PostLinks.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/post_links'
esquema_post_links = StructType([
    StructField("_Id", LongType(), True),
    StructField("_CreationDate", TimestampType(), True),
    StructField("_PostId", LongType(), True),
    StructField("_RelatedPostId", LongType(), True),
    StructField("_LinkTypeId", IntegerType(), True)
])

# COMMAND ----------

df_post_links = spark.read.format("xml").options(rootTag="post_links", rowTag="row").load(dir_origem, schema = esquema_post_links)

# COMMAND ----------

print(df_post_links.count())
# arquivo menor = 1.916.498  rows
# arquivo maior = 43.726.638 rows
display(df_post_links)

# COMMAND ----------

df_post_links_with_dataImport = df_post_links.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_post_links_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "post_links",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

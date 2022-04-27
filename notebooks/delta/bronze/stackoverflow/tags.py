# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Tags.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/tags'
esquema_tags = StructType([
    StructField("_Id", LongType(), True),
    StructField("_TagName", StringType(), True),
    StructField("_Count", IntegerType(), True),
    StructField("_ExcerptPostId", LongType(), True),
    StructField("_WikiPostId", LongType(), True)
])

# COMMAND ----------

df_tags = spark.read.format("xml").options(rootTag="tags", rowTag="row").load(dir_origem, schema = esquema_tags)

# COMMAND ----------

print(df_tags.count())
# arquivo menor =  rows
# arquivo maior = 43.726.638 rows
display(df_tags)

# COMMAND ----------

df_tags_with_dataImport = df_tags.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_tags_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "tags",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

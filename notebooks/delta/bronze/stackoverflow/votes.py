# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Votes.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/votes'
esquema_votes = StructType([
    StructField("_Id", LongType(), True),
    StructField("_PostId", LongType(), True),
    StructField("_VoteTypeId", IntegerType(), True),
    StructField("_UserId", IntegerType(), True),
    StructField("_CreationDate", TimestampType(), True)
])

# COMMAND ----------

df_votes = spark.read.format("xml").options(rootTag="votes", rowTag="row").load(dir_origem, schema = esquema_votes)

# COMMAND ----------

print(df_votes.count())
# arquivo menor =  rows
# arquivo maior = 227.298.899 rows
display(df_votes)

# COMMAND ----------

df_votes_with_dataImport = df_votes.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_votes_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "votes",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
display(spark.sql("SELECT * FROM {esquema}.{tabela} LIMIT 100".format(**param)))

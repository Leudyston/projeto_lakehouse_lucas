# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

dir_origem = '/mnt/raw/processing/stackoverflow/Badges.xml'
dir_destino = '/mnt/delta/bronze/stackoverflow/badges'
esquema_badges = StructType([
    StructField("_Id", LongType(), True),
    StructField("_UserId", LongType(), True),
    StructField("_Name", StringType(), True),
    StructField("_Date", TimestampType(), True),
    StructField("_Class", IntegerType(), True),
    StructField("_TagBased", StringType(), True)
])

# COMMAND ----------

df_badges = spark.read.format("xml").options(rootTag="badges", rowTag="row").load(dir_origem, schema = esquema_badges)

# COMMAND ----------

print(df_badges.count())
# arquivo menor = 1.916.498  rows
# arquivo maior = 43.726.638 rows

# COMMAND ----------

df_badges_with_dataImport = df_badges.withColumn('DATA_IMPORT', date_format(lit(datetime.now()), "YYYY-MM-dd"))

# COMMAND ----------

df_badges_with_dataImport.write.format("delta").mode("overwrite").save(dir_destino)

# COMMAND ----------

param = {
  "local": dir_destino,
  "tabela": "badges",
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
spark.sql("REFRESH TABLE {esquema}.{tabela}".format(**param))

# COMMAND ----------



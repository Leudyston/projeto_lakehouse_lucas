# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

param = {
    "tabela_gold": "dim_medalha",
    "esquema_gold": "gold",
    "local_tabela_gold": "/mnt/gold/dimensao/dim_medalha"
}

# COMMAND ----------

df_silver_badges = spark.table('silver.medalhas')

# COMMAND ----------

display(df_silver_badges)

# COMMAND ----------

df_seleciona_colunas = df_silver_badges.select(col('NAME_BADGE').alias('NOME_MEDALHA')).distinct().orderBy('NOME_MEDALHA')

# COMMAND ----------

try:
    df_dim = spark.table(param['esquema_gold'] + '.' + param['tabela_gold'])
    existe_dim = True
except AnalysisException as ex:
    existe_dim = False

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")

if existe_dim:
    ultima_sk = df_dim.select('SK_MEDALHA').agg({'SK_MEDALHA':'max'}).collect()[0][0]
    
    df_gold_registros_que_ja_existem = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'NOME_MEDALHA', 'inner')
            .select('SK_TAG', 'stg.*', 'DT_INSERCAO_GOLD', dt_atual.alias('DT_ATUALIZACAO_GOLD'))
    )
    
    df_gold_registros_novos = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'NOME_MEDALHA', 'leftanti')
            .select(
                (row_number().over(Window.partitionBy().orderBy('NOME_MEDALHA')) + ultima_sk).alias('SK_MEDALHA'), 
                'stg.*', 
                dt_atual.alias('DT_INSERCAO_GOLD'), 
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )
    
    df_gold = df_gold_registros_que_ja_existem.union(df_gold_registros_novos).orderBy('SK_MEDALHA')
    
else:
    df_gold = (
        df_seleciona_colunas
            .select(
                (row_number().over(Window.partitionBy().orderBy('NOME_MEDALHA'))).alias('SK_MEDALHA'),
                '*',
                dt_atual.alias('DT_INSERCAO_GOLD'),
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )

# COMMAND ----------

display(df_gold)

# COMMAND ----------

if existe_dim:
    tabela_delta = DeltaTable.forPath(spark, param["local_tabela_gold"])
    dicionario = {col : f'changed.{col}' for col in df_gold.columns}
    
    (
        tabela_delta.alias('original')
            .merge(df_gold.alias('changed'),
                "original.SK_TAG == changed.SK_TAG"
            )
            .whenNotMatchedInsert(values = dicionario)
            .whenMatchedUpdateAll()
            .execute()
    )
else:
    df_gold.write.format("delta").mode("overwrite").save(param["local_tabela_gold"])
    spark.sql("DROP TABLE IF EXISTS {esquema_gold}.{tabela_gold}".format(**param))
    spark.sql("CREATE TABLE {esquema_gold}.{tabela_gold} USING DELTA LOCATION '{local_tabela_gold}'".format(**param))    

# COMMAND ----------

display(spark.sql("SELECT COUNT(1) FROM {esquema_gold}.{tabela_gold}".format(**param)))
display(spark.sql("SELECT * FROM {esquema_gold}.{tabela_gold}".format(**param)))

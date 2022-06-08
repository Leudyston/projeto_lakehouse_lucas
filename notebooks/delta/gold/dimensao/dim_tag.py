# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

param = {
    "tabela_gold": "dim_tag",
    "esquema_gold": "gold",
    "local_tabela_gold": "/mnt/gold/dimensao/dim_tag"
}

# COMMAND ----------

df_silver_tags = spark.table('silver.tags')

# COMMAND ----------

display(df_silver_tags)

# COMMAND ----------

df_seleciona_colunas = df_silver_tags.select('ID_TAG', col('TAG_NAME').alias('NOME_TAG')).distinct().orderBy('ID_TAG')

# COMMAND ----------

try:
    df_dim = spark.table(param['esquema_gold'] + '.' + param['tabela_gold'])
    existe_dim = True
except AnalysisException as ex:
    existe_dim = False

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")

if existe_dim:
    ultima_sk = df_dim.select('SK_TAG').agg({'SK_TAG':'max'}).collect()[0][0]
    
    df_gold_atualiza_registros_que_ja_existem = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'ID_TAG', 'inner')
            .select('SK_TAG', 'stg.*', 'DT_INSERCAO_GOLD', dt_atual.alias('DT_ATUALIZACAO_GOLD'))
    )
    
    df_gold_adiciona_registros_novos = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'ID_TAG', 'leftanti')
            .select(
                (row_number().over(Window.partitionBy().orderBy('ID_TAG')) + ultima_sk).alias('SK_TAG'), 
                'stg.*', 
                dt_atual.alias('DT_INSERCAO_GOLD'), 
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )
    
    df_gold = df_gold_atualiza_registros_que_ja_existem.union(df_gold_adiciona_registros_novos).orderBy('SK_TAG')
    
else:
    df_gold = (
        df_seleciona_colunas
            .select(
                (row_number().over(Window.partitionBy().orderBy('ID_TAG'))).alias('SK_TAG'),
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

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE silver.medalhas;
# MAGIC DROP TABLE silver.comentarios;
# MAGIC DROP TABLE silver.postagem_links;
# MAGIC DROP TABLE silver.postagem_revisoes;
# MAGIC DROP TABLE silver.postagem_tags;
# MAGIC DROP TABLE silver.postagens;
# MAGIC DROP TABLE silver.tipo_votos;
# MAGIC DROP TABLE silver.usuarios;
# MAGIC DROP TABLE silver.votos;

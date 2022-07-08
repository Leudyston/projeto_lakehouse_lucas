# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

# COMMAND ----------

param = {
    "tabela_gold": "dim_tag",
    "esquema_gold": "gold",
    "coluna_sk": "SK_TAG",
    "local_tabela_gold": "/mnt/gold/usuario/dim_tag"
}

# COMMAND ----------

df_silver_tags = spark.table('silver.tags')

# COMMAND ----------

# display(df_silver_tags)

# COMMAND ----------

df_seleciona_colunas = df_silver_tags.select('ID_TAG', col('NOME_TAG').alias('NOME_TAG')).distinct().orderBy('ID_TAG')

# COMMAND ----------

try:
    df_dim = spark.table(param['esquema_gold'] + '.' + param['tabela_gold'])
    existe_dim = True
except AnalysisException as ex:
    existe_dim = False
    
print(f'Já existe gold? {existe_dim}')

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")
coluna_chave = 'ID_TAG'
coluna_sk = 'SK_TAG'
    
if existe_dim:
    ultima_sk = df_dim.select(coluna_sk).agg({coluna_sk:'max'}).collect()[0][0]
    
    df_nova_gold_registros_que_ja_existem_pra_atualizar = (
        df_seleciona_colunas.alias('novos_dados')
            .join(df_dim, coluna_chave, 'inner')
            .select(
                df_dim[coluna_sk],
                'novos_dados.*', 
                df_dim['DT_INSERCAO_GOLD'], 
                dt_atual.alias('DT_ATUALIZACAO_GOLD'))
    )
    
    df_nova_gold_registros_novos_pra_adicionar = (
        df_seleciona_colunas.alias('novos_dados')
            .join(df_dim, coluna_chave, 'leftanti')
            .select(
                (row_number().over(Window.partitionBy().orderBy(coluna_chave)) + ultima_sk).alias(coluna_sk), 
                'novos_dados.*', 
                dt_atual.alias('DT_INSERCAO_GOLD'), 
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )
    
    df_antiga_gold_registros_que_permanecem_sem_alteracao = (
        df_dim.alias('original')
            .join(df_seleciona_colunas, coluna_chave, 'leftanti')
            .select(
                'original.*'
            )
    )
    
    df_gold = (
        df_nova_gold_registros_que_ja_existem_pra_atualizar
            .union(df_nova_gold_registros_novos_pra_adicionar
                .union(df_antiga_gold_registros_que_permanecem_sem_alteracao)
            )
            .orderBy(coluna_sk)
    )
    
else:
    df_gold = (
        df_seleciona_colunas
            .select(
                (row_number().over(Window.partitionBy().orderBy(coluna_chave))).alias(coluna_sk),
                '*',
                dt_atual.alias('DT_INSERCAO_GOLD'),
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )

# COMMAND ----------

# display(df_gold)

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(param["local_tabela_gold"])
spark.sql("DROP TABLE IF EXISTS {esquema_gold}.{tabela_gold}".format(**param))
spark.sql("CREATE TABLE {esquema_gold}.{tabela_gold} USING DELTA LOCATION '{local_tabela_gold}'".format(**param))    

# COMMAND ----------

display(spark.sql("OPTIMIZE {esquema_gold}.{tabela_gold} ZORDER BY ({coluna_sk})".format(**param)))

# COMMAND ----------

display(spark.sql("SELECT COUNT(1) FROM {esquema_gold}.{tabela_gold}".format(**param)))
display(spark.sql("SELECT * FROM {esquema_gold}.{tabela_gold}".format(**param)))

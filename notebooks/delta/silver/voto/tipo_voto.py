# Databricks notebook source
from pyspark.sql.functions import date_format, from_utc_timestamp, current_timestamp, col, trim
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

param = {
    "tabela_bronze": "vote_types",
    "esquema_bronze": "bronze",
    "tabela_silver": "tipo_voto",
    "esquema_silver": "silver",
    "local_tabela_silver": "/mnt/silver/gerencial/tipo_voto"
}

# COMMAND ----------

df = spark.table("{esquema_bronze}.{tabela_bronze}".format(**param))

# COMMAND ----------

# df.printSchema()
# display(df)

# COMMAND ----------

df_ajusta_colunas = ( df
        .withColumnRenamed("Id", "ID_TIPO_VOTO")
        .withColumnRenamed("Name", "NOME_TIPO_VOTO")
        .select('ID_TIPO_VOTO', 'NOME_TIPO_VOTO')
)

# COMMAND ----------

# display(df_ajusta_colunas)

# COMMAND ----------

try:
    df_antiga_silver = spark.table(param['esquema_silver'] + '.' + param['tabela_silver'])
    ja_existe_silver = True
except AnalysisException as ex:
    ja_existe_silver = False
    
print(f'Já existe silver? {ja_existe_silver}')

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")

if ja_existe_silver:
    
    coluna_chave = 'ID_TIPO_VOTO'
    
    df_nova_silver_registros_que_ja_existem_pra_atualizar = (
        df_ajusta_colunas.alias('nova_silver')
            .join(df_antiga_silver, coluna_chave, 'inner')
            .select(
                'nova_silver.*', 
                df_antiga_silver['DT_INSERCAO_SILVER'], 
                dt_atual.alias('DT_ATUALIZACAO_SILVER')
            )
    )
    
    df_nova_silver_registros_novos_pra_adicionar = (
        df_ajusta_colunas.alias('nova_silver')
            .join(df_antiga_silver, coluna_chave, 'leftanti')
            .select(
                'nova_silver.*', 
                dt_atual.alias('DT_INSERCAO_SILVER'), 
                dt_atual.alias('DT_ATUALIZACAO_SILVER')
            )
    )
    
    df_antiga_silver_registros_que_permanecem_sem_alteracao = (
        df_antiga_silver.alias('antiga_silver')
            .join(df_ajusta_colunas, coluna_chave, 'leftanti')
            .select(
                'antiga_silver.*'
            )
    )
    
    df_nova_silver = (
        df_nova_silver_registros_que_ja_existem_pra_atualizar
            .union(df_nova_silver_registros_novos_pra_adicionar
                .union(df_antiga_silver_registros_que_permanecem_sem_alteracao)
            )
            .orderBy(coluna_chave)
    )
    
else:
    df_nova_silver = (
        df_ajusta_colunas
            .select(
                '*',
                dt_atual.alias('DT_INSERCAO_SILVER'),
                dt_atual.alias('DT_ATUALIZACAO_SILVER')
            )
    )

# COMMAND ----------

# display(df_nova_silver)

# COMMAND ----------

df_nova_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(param["local_tabela_silver"])

spark.sql("DROP TABLE IF EXISTS {esquema_silver}.{tabela_silver}".format(**param))
spark.sql("CREATE TABLE {esquema_silver}.{tabela_silver} USING DELTA LOCATION '{local_tabela_silver}'".format(**param))

# COMMAND ----------

display(spark.sql("SELECT COUNT(1) FROM {esquema_silver}.{tabela_silver}".format(**param)))
display(spark.sql("SELECT * FROM {esquema_silver}.{tabela_silver}".format(**param)))

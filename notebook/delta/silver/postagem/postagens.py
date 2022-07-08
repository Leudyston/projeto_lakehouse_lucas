# Databricks notebook source
from pyspark.sql.functions import date_format, from_utc_timestamp, current_timestamp, col, date_format
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

param = {
    "tabela_bronze": "posts",
    "esquema_bronze": "bronze",
    "tabela_silver": "postagens",
    "esquema_silver": "silver",
    "local_tabela_silver": "/mnt/silver/gerencial/postagens"
}

# COMMAND ----------

df = spark.table("{esquema_bronze}.{tabela_bronze}".format(**param))

# COMMAND ----------

# df.printSchema()
# display(df)

# COMMAND ----------

df_ajusta_colunas = (
    df
        .withColumnRenamed("_Id", "ID_POSTAGEM")
        .withColumnRenamed("_PostTypeId", "ID_TIPO_POSTAGEM")
        .withColumnRenamed("_AcceptedAnswerId", "ID_RESPOSTA_ACEITA")
        .withColumn("DT_CRIACAO_POSTAGEM", date_format(col("_CreationDate"), "yyyy-MM-dd HH:mm:ss"))
        .withColumnRenamed("_Score", "PONTUACAO")
        .withColumnRenamed("_ViewCount", "QTD_VISUALIZACAO")
        .withColumnRenamed("_Body", "CORPO_TEXTO")
        .withColumnRenamed("_OwnerUserId", "ID_USUARIO_DONO")
        .withColumnRenamed("_LastEditorUserId", "ID_USUARIO_ULTIMO_EDITOR")
        .withColumnRenamed("_LastEditorDisplayName", "NOME_ULTIMO_EDITOR")
        .withColumn("DT_ULTIMA_EDICAO", date_format(col("_LastEditDate"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("DT_ULTIMA_ATIVIDADE", date_format(col("_LastActivityDate"), "yyyy-MM-dd HH:mm:ss"))
        .withColumnRenamed("_Title", "TITULO")
        .withColumnRenamed("_Tags", "TAGS")
        .withColumnRenamed("_AnswerCount", "QTD_RESPOSTA")
        .withColumnRenamed("_CommentCount", "QTD_COMENTARIO")
        .withColumnRenamed("_FavoriteCount", "QTD_FAVORITO")
        .select('ID_POSTAGEM', 'ID_TIPO_POSTAGEM', 'ID_RESPOSTA_ACEITA', 'DT_CRIACAO_POSTAGEM', 'PONTUACAO', 'QTD_VISUALIZACAO', 'CORPO_TEXTO', 
                'ID_USUARIO_DONO', 'ID_USUARIO_ULTIMO_EDITOR', 'NOME_ULTIMO_EDITOR', 'DT_ULTIMA_EDICAO', 
                'DT_ULTIMA_ATIVIDADE', 'TITULO', 'TAGS', 'QTD_RESPOSTA', 'QTD_COMENTARIO', 
                'QTD_FAVORITO')
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
    
    coluna_chave = 'ID_POSTAGEM'
    
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

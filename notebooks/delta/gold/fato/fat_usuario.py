# Databricks notebook source
from pyspark.sql.functions import *
import datetime

# COMMAND ----------

df_usuarios = spark.table("silver.usuarios")
df_postagens = spark.table("silver.postagens")
df_comentarios = spark.table("silver.comentarios")
df_votos = spark.table("silver.votos")
df_votos = spark.table("silver.tipo_voto")

# COMMAND ----------

# display(df_usuarios)
# display(df_postagens)

# COMMAND ----------

display(df_comentarios)

# COMMAND ----------

kpi_qtd_comentarios_realizados_em_posts = df_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_COMENTARIO").alias("QTD_COMENTARIOS_REALIZADOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_pontos_recebidos_em_comentarios = df_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("PONTUACAO").alias("QTD_PONTOS_RECEBIDOS_EM_COMENTARIOS")).orderBy("ID_USUARIO")

display(kpi_qtd_pontos_recebidos_em_comentarios)

# COMMAND ----------

kpi_qtd_questoes_realizadas = df_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_QUESTOES_REALIZADAS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_realizadas_em_posts = df_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 2 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_RESPOSTAS_REALIZADAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_realizadas_aceitas_em_posts = df_postagens.alias("a").join(df_postagens.alias("b"), col("a.ID_POSTAGEM") == col("b.ID_RESPOSTA_ACEITA"), "inner").select("a.*").withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_RESPOSTAS_REALIZADAS_ACEITAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_recebidas_em_posts = df_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_RESPOSTA").alias("QTD_RESPOSTAS_RECEBIDAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_comentarios_recebidos_em_posts = df_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_COMENTARIO").alias("QTD_COMENTARIOS_RECEBIDOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_favoritos_recebidos_em_posts = df_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_FAVORITO").alias("QTD_FAVORITOS_RECEBIDOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_comentarios_realizados_em_posts = df_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_COMENTARIO").alias("QTD_COMENTARIOS_REALIZADOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_pontos_recebidos_em_comentarios = df_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("PONTUACAO").alias("QTD_PONTOS_RECEBIDOS_EM_COMENTARIOS")).orderBy("ID_USUARIO")

# COMMAND ----------

fat_usuario_base = (
    df_usuarios
        .selectExpr("ID_USUARIO", "VISUALIZACAO AS QTD_POSTAGENS_VISUALIZADAS", "VOTOS_PARA_CIMA AS QTD_VOTOS_UP_REALIZADOS", "VOTOS_PARA_BAIXO AS QTD_VOTOS_DOWN_REALIZADOS")
        .join(kpi_qtd_questoes_realizadas, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_realizadas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_realizadas_aceitas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_recebidas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_comentarios_recebidos_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_favoritos_recebidos_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_comentarios_realizados_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_pontos_recebidos_em_comentarios, "ID_USUARIO", "left")
        .select("ID_USUARIO", "QTD_POSTAGENS_VISUALIZADAS", "QTD_VOTOS_UP_REALIZADOS", "QTD_VOTOS_DOWN_REALIZADOS", "QTD_QUESTOES_REALIZADAS", "QTD_RESPOSTAS_REALIZADAS_EM_POSTS",
               "QTD_RESPOSTAS_REALIZADAS_ACEITAS_EM_POSTS", "QTD_RESPOSTAS_RECEBIDAS_EM_POSTS", "QTD_COMENTARIOS_RECEBIDOS_EM_POSTS", "QTD_FAVORITOS_RECEBIDOS_EM_POSTS",
               "QTD_COMENTARIOS_REALIZADOS_EM_POSTS", "QTD_PONTOS_RECEBIDOS_EM_COMENTARIOS")
)

display(fat_usuario_base)

# COMMAND ----------

fat_usuario_base.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.ID_USUARIO_DONO, COUNT(*)
# MAGIC from silver.postagens as a
# MAGIC   join silver.postagens as b
# MAGIC     on a.ID_POSTAGEM = b.ID_RESPOSTA_ACEITA
# MAGIC group by a.ID_USUARIO_DONO
# MAGIC order by a.ID_USUARIO_DONO

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver.usuarios

# COMMAND ----------



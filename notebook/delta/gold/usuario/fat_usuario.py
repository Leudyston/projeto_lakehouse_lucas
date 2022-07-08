# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime

# COMMAND ----------

param = {
    "tabela_gold": "fat_usuario",
    "esquema_gold": "gold",
    "local_tabela_gold": "/mnt/gold/usuario/fat_usuario"
}

# COMMAND ----------

silver_usuarios = spark.table("silver.usuarios")
silver_postagens = spark.table("silver.postagens")
silver_comentarios = spark.table("silver.comentarios")
silver_postagem_tags = spark.table("silver.postagem_tags")

dim_usuario = spark.table("gold.dim_usuario")
dim_medalha = spark.table("gold.dim_medalha")
dim_postagem = spark.table("gold.dim_postagem")
dim_tag = spark.table("gold.dim_tag")

# COMMAND ----------

display(silver_usuarios)
display(silver_postagens)
display(silver_comentarios)
display(silver_postagem_tags)

display(dim_usuario)
display(dim_medalha)
display(dim_postagem)
display(dim_tag)

# COMMAND ----------

# DBTITLE 1,Monta dataframes auxiliares para trazer as chaves das Dimensões
df_tag_mais_utilizada_em_questoes_realizadas = (
    silver_postagem_tags
        .groupBy("ID_USUARIO", "ID_TAG").agg(count("ID_USUARIO").alias("QTD_VEZES_TAG_MENCIONADA"))
        .withColumn("n_linha", row_number().over(Window.partitionBy("ID_USUARIO").orderBy(col("QTD_VEZES_TAG_MENCIONADA").desc())))
        .where("n_linha == 1")
        .join(dim_tag, "ID_TAG", "inner")
        .selectExpr("ID_USUARIO", "SK_TAG AS SK_TAG_MAIS_UTILIZADA_EM_QUESTOES_REALIZADAS")
)

df_ultima_postagem_criada = (
    silver_postagens.alias('silver')
        .groupBy("ID_POSTAGEM", "ID_USUARIO_DONO").agg(max("DT_CRIACAO_POSTAGEM").alias("ULTIMA_DT_CRIACAO_POSTAGEM"))
        .withColumn("n_linha", row_number().over(Window.partitionBy("ID_USUARIO_DONO").orderBy(col("ULTIMA_DT_CRIACAO_POSTAGEM").desc())))
        .where("n_linha == 1")
        .join(dim_postagem, "ID_POSTAGEM", "inner")
        .selectExpr("silver.ID_USUARIO_DONO as ID_USUARIO", "SK_POSTAGEM AS SK_ULTIMA_POSTAGEM_CRIADA")
)

# COMMAND ----------

# DBTITLE 1,Monta dataframes auxiliares para trazer as chaves das Dimensões
df_medalha_ouro_mais_recebida = spark.sql('''
    SELECT ID_USUARIO, NOME_MEDALHA
    FROM (
      SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS n_linha
      FROM silver.usuario_medalhas 
      WHERE CLASSIFICACAO_MEDALHA = 1 
      GROUP BY ID_USUARIO, NOME_MEDALHA
    ) AS a
    WHERE n_linha = 1
    ORDER BY ID_USUARIO
''').join(dim_medalha, "NOME_MEDALHA", "inner").selectExpr("ID_USUARIO", "SK_MEDALHA AS SK_MEDALHA_OURO_MAIS_RECEBIDA")

df_medalha_prata_mais_recebida = spark.sql('''
    SELECT ID_USUARIO, NOME_MEDALHA
    FROM (
      SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS n_linha
      FROM silver.usuario_medalhas 
      WHERE CLASSIFICACAO_MEDALHA = 2 
      GROUP BY ID_USUARIO, NOME_MEDALHA
    ) AS a
    WHERE n_linha = 1
    ORDER BY ID_USUARIO
''').join(dim_medalha, "NOME_MEDALHA", "inner").selectExpr("ID_USUARIO", "SK_MEDALHA AS SK_MEDALHA_PRATA_MAIS_RECEBIDA")

df_medalha_bronze_mais_recebida = spark.sql('''
    SELECT ID_USUARIO, NOME_MEDALHA
    FROM (
      SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS n_linha
      FROM silver.usuario_medalhas 
      WHERE CLASSIFICACAO_MEDALHA = 3 
      GROUP BY ID_USUARIO, NOME_MEDALHA
    ) AS a
    WHERE n_linha = 1
    ORDER BY ID_USUARIO
''').join(dim_medalha, "NOME_MEDALHA", "inner").selectExpr("ID_USUARIO", "SK_MEDALHA AS SK_MEDALHA_BRONZE_MAIS_RECEBIDA")

# COMMAND ----------

# DBTITLE 1,Vinculas as Dimensões ao novo dataframe
fat_usuario_com_dimensoes = (
    silver_usuarios.alias("base")
        .join(df_tag_mais_utilizada_em_questoes_realizadas, "ID_USUARIO", "left")
        .join(df_ultima_postagem_criada, "ID_USUARIO", "left")
        .join(df_medalha_ouro_mais_recebida, "ID_USUARIO", "left")
        .join(df_medalha_prata_mais_recebida, "ID_USUARIO", "left")
        .join(df_medalha_bronze_mais_recebida, "ID_USUARIO", "left")
        .join(dim_usuario, "ID_USUARIO", "left")
        .selectExpr("base.*", "SK_USUARIO", "SK_TAG_MAIS_UTILIZADA_EM_QUESTOES_REALIZADAS", "SK_ULTIMA_POSTAGEM_CRIADA", 
                    "SK_MEDALHA_OURO_MAIS_RECEBIDA", "SK_MEDALHA_PRATA_MAIS_RECEBIDA", "SK_MEDALHA_BRONZE_MAIS_RECEBIDA")
).cache()

display(fat_usuario_com_dimensoes)

# COMMAND ----------

# DBTITLE 1,Monta dataframes auxiliares para trazer os KPI's
kpi_qtd_questoes_realizadas = silver_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_QUESTOES_REALIZADAS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_realizadas_em_posts = silver_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 2 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_RESPOSTAS_REALIZADAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_realizadas_aceitas_em_posts = silver_postagens.alias("a").join(silver_postagens.alias("b"), col("a.ID_POSTAGEM") == col("b.ID_RESPOSTA_ACEITA"), "inner").select("a.*").withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_RESPOSTAS_REALIZADAS_ACEITAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_respostas_recebidas_em_posts = silver_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_RESPOSTA").alias("QTD_RESPOSTAS_RECEBIDAS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_comentarios_recebidos_em_posts = silver_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_COMENTARIO").alias("QTD_COMENTARIOS_RECEBIDOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_favoritos_recebidos_em_posts = silver_postagens.withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO").where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("QTD_FAVORITO").alias("QTD_FAVORITOS_RECEBIDOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_comentarios_realizados_em_posts = silver_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(count("ID_COMENTARIO").alias("QTD_COMENTARIOS_REALIZADOS_EM_POSTS")).orderBy("ID_USUARIO")
kpi_qtd_pontos_recebidos_em_comentarios = silver_comentarios.where("ID_USUARIO IS NOT NULL").groupBy("ID_USUARIO").agg(sum("PONTUACAO").alias("QTD_PONTOS_RECEBIDOS_EM_COMENTARIOS")).orderBy("ID_USUARIO")

# COMMAND ----------

# DBTITLE 1,Vincula os KPIs ao novo dataframe
fat_usuario_com_kpis = (
    fat_usuario_com_dimensoes.alias("base")
        .join(kpi_qtd_questoes_realizadas, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_realizadas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_realizadas_aceitas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_respostas_recebidas_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_comentarios_recebidos_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_favoritos_recebidos_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_comentarios_realizados_em_posts, "ID_USUARIO", "left")
        .join(kpi_qtd_pontos_recebidos_em_comentarios, "ID_USUARIO", "left")
        .selectExpr("base.SK_USUARIO", "base.SK_TAG_MAIS_UTILIZADA_EM_QUESTOES_REALIZADAS", "base.SK_ULTIMA_POSTAGEM_CRIADA", 
                "base.SK_MEDALHA_OURO_MAIS_RECEBIDA", "base.SK_MEDALHA_PRATA_MAIS_RECEBIDA", "base.SK_MEDALHA_BRONZE_MAIS_RECEBIDA",
                "base.VISUALIZACAO AS QTD_POSTAGENS_VISUALIZADAS", "base.VOTOS_PARA_CIMA AS QTD_VOTOS_UP_REALIZADOS", 
                "base.VOTOS_PARA_BAIXO AS QTD_VOTOS_DOWN_REALIZADOS", "QTD_QUESTOES_REALIZADAS", "QTD_RESPOSTAS_REALIZADAS_EM_POSTS", "QTD_RESPOSTAS_REALIZADAS_ACEITAS_EM_POSTS", 
                "QTD_RESPOSTAS_RECEBIDAS_EM_POSTS", "QTD_COMENTARIOS_RECEBIDOS_EM_POSTS", "QTD_FAVORITOS_RECEBIDOS_EM_POSTS",
                "QTD_COMENTARIOS_REALIZADOS_EM_POSTS", "QTD_PONTOS_RECEBIDOS_EM_COMENTARIOS")
).cache()

display(fat_usuario_com_kpis)

# COMMAND ----------

df_gold = fat_usuario_com_kpis

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(param["local_tabela_gold"])

spark.sql("DROP TABLE IF EXISTS {esquema_gold}.{tabela_gold}".format(**param))
spark.sql("CREATE TABLE {esquema_gold}.{tabela_gold} USING DELTA LOCATION '{local_tabela_gold}'".format(**param))    

# COMMAND ----------

display(spark.sql("OPTIMIZE {esquema_gold}.{tabela_gold}".format(**param)))

# COMMAND ----------

display(spark.sql("SELECT COUNT(1) FROM {esquema_gold}.{tabela_gold}".format(**param)))
display(spark.sql("SELECT * FROM {esquema_gold}.{tabela_gold}".format(**param)))

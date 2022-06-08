# Databricks notebook source
from pyspark.sql.functions import *
import datetime

# COMMAND ----------

df_usuarios = spark.table("silver.usuarios")
df_postagens = spark.table("silver.postagens")

# COMMAND ----------

display(df_usuarios)
display(df_postagens)

# COMMAND ----------

kpi_qtd_questoes = (
    df_postagens
        .withColumnRenamed("ID_USUARIO_DONO", "ID_USUARIO")
        .where("ID_TIPO_POSTAGEM == 1 AND ID_USUARIO IS NOT NULL")
        .groupBy("ID_USUARIO")
        .agg(count("ID_POSTAGEM").alias("QTD_QUESTOES"))
        .orderBy("ID_USUARIO")
)
    
display(kpi_qtd_questoes)

# COMMAND ----------

kpi_qtd_questoes =  df_usuarios.join(df_postagens.where("ID_TIPO_POSTAGEM == 1"), df_usuarios["ID_USUARIO"] == df_postagens["ID_USUARIO_DONO"], "inner").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_QUESTOES"))
kpi_qtd_respostas = df_usuarios.join(df_postagens.where("ID_TIPO_POSTAGEM == 2"), df_usuarios["ID_USUARIO"] == df_postagens["ID_USUARIO_DONO"], "inner").groupBy("ID_USUARIO").agg(count("ID_POSTAGEM").alias("QTD_RESPOSTAS"))

# COMMAND ----------

display(kpi_qtd_questoes)
display(kpi_qtd_respostas)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.usuarios

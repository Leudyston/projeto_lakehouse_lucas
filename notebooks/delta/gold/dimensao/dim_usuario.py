# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

param = {
    "tabela_gold": "dim_usuario",
    "esquema_gold": "gold",
    "local_tabela_gold": "/mnt/gold/dimensao/dim_usuario"
}

# COMMAND ----------

df_silver_usuario = spark.table('silver.usuarios')

# COMMAND ----------

display(df_silver_usuario)

# COMMAND ----------

df_seleciona_colunas = df_silver_usuario.select('ID_USER', 'DISPLAY_NAME', 'REPUTATION', 'CREATION_DATE', 'LAST_ACCESS_DATE', 'PROFILE_IMAGE_URL').distinct().orderBy('ID_USER')

# COMMAND ----------

try:
    df_dim = spark.table(param['esquema_gold'] + '.' + param['tabela_gold'])
    existe_dim = True
except AnalysisException as ex:
    existe_dim = False

# COMMAND ----------

# DBTITLE 1,Teste de inserção de novo registro na dimensão
## Essa célula irá testar se o dataframe da célula abaixo chamado "df_gold_registros_novos" irá funcionar. Então, caso queira realizar esse teste, basta descomentar todas as linhas abaixo.
# teste_linhas = [(-2999,'Teste TCC',3,'2022-01-31 19:45:27','2022-01-31 19:45:27','null')]
# teste_colunas = ["ID_USER", "DISPLAY_NAME", "REPUTATION", "CREATION_DATE", "LAST_ACCESS_DATE", "PROFILE_IMAGE_URL"]
# df_teste = spark.createDataFrame(data=teste_linhas, schema=teste_colunas)
# df_seleciona_colunas = df_teste.union(df_seleciona_colunas)
# display(df_seleciona_colunas)

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")

if existe_dim:
    ultima_sk = df_dim.select('SK_USUARIO').agg({'SK_USUARIO':'max'}).collect()[0][0]
    
    # 
    df_gold_registros_que_ja_existem = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'ID_USER', 'inner')
            .select('SK_USUARIO', 'stg.*', 'DT_INSERCAO_GOLD', dt_atual.alias('DT_ATUALIZACAO_GOLD'))
    )
    
    df_gold_registros_novos = (
        df_seleciona_colunas.alias('stg')
            .join(df_dim, 'ID_USER', 'leftanti')
            .select(
                (row_number().over(Window.partitionBy().orderBy('ID_USER')) + ultima_sk).alias('SK_USUARIO'), 
                'stg.*', 
                dt_atual.alias('DT_INSERCAO_GOLD'), 
                dt_atual.alias('DT_ATUALIZACAO_GOLD')
            )
    )
    
    df_gold = df_gold_registros_que_ja_existem.union(df_gold_registros_novos).orderBy('SK_USUARIO')
    
else:
    df_gold = (
        df_seleciona_colunas
            .select(
                (row_number().over(Window.partitionBy().orderBy('ID_USER'))).alias('SK_USUARIO'),
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
                "original.SK_USUARIO == changed.SK_USUARIO"
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

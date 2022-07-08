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
    "coluna_sk": "SK_USUARIO",  
    "local_tabela_gold": "/mnt/gold/usuario/dim_usuario"
}

# COMMAND ----------

df_silver_usuario = spark.table('silver.usuarios')

# COMMAND ----------

# display(df_silver_usuario)

# COMMAND ----------

df_seleciona_colunas = df_silver_usuario.select('ID_USUARIO', 'NOME_EXIBICAO', 'REPUTACAO', 'DT_CRIACAO', 'DT_ULTIMO_ACESSO', 'URL_IMAGEM_PERFIL').distinct().orderBy('ID_USUARIO')

# COMMAND ----------

try:
    df_dim = spark.table(param['esquema_gold'] + '.' + param['tabela_gold'])
    existe_dim = True
except AnalysisException as ex:
    existe_dim = False
    
print(f'Já existe gold? {existe_dim}')

# COMMAND ----------

# DBTITLE 1,Teste de inserção de novo registro na dimensão
## Essa célula irá testar se o dataframe da célula abaixo chamado "df_gold_registros_novos" irá funcionar. Então, caso queira realizar esse teste, basta descomentar todas as linhas abaixo.
# teste_linhas = [(-3000,'Teste New Insert',3,'2022-01-31 19:45:27','2022-01-31 19:45:27','null'),(-1007,'Teste Update 2',3,'2022-01-31 19:45:27','2022-01-31 19:45:27','null')]
# teste_colunas = ['ID_USUARIO', 'NOME_EXIBICAO', 'REPUTACAO', 'DT_CRIACAO', 'DT_ULTIMO_ACESSO', 'URL_IMAGEM_PERFIL']
# df_teste = spark.createDataFrame(data=teste_linhas, schema=teste_colunas)
# df_seleciona_colunas = df_teste.union(df_seleciona_colunas.where('ID_USUARIO <> -1008'))
# display(df_seleciona_colunas)

# COMMAND ----------

dt_atual = date_format(from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"), "yyyy-MM-dd HH:mm:ss")
coluna_chave = 'ID_USUARIO'
coluna_sk = 'SK_USUARIO'
    
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
display(spark.sql("SELECT * FROM {esquema_gold}.{tabela_gold} ORDER BY 1".format(**param)))

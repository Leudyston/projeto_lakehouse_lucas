# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Badges

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.badges

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT _Name), COUNT(DISTINCT _Id), COUNT(DISTINCT _UserId), COUNT(DISTINCT _Class), COUNT(DISTINCT _TagBased) FROM bronze.badges
# MAGIC 
# MAGIC -- Pela quantidade diferente e bem discrepante entre "_Name" e "_Id" entendemos que esse ID não faz referência a um 'cadastros de badges'. Logo, essa deve ser uma tabela que apresenta o momento em que determinado usuário ganha uma "medalha". 
# MAGIC -- Por não ter um 'IdName' não vale a pena criar uma tabela de Nomes únicos na silver. Mas sim, depois criar uma dimensão de badges com um ID artificial.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _Name, _Class FROM bronze.badges GROUP BY _Name, _Class
# MAGIC 
# MAGIC -- Não foi constatado nenhuma relação aparente entre essas duas colunas. 
# MAGIC -- E "_Class" por não se tratar de uma coluna autoexplicativa, da qual poderíamos dar uma utilidade a ela, não vamos levá-la para a Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a._TagBased, COUNT(b._TagName) QTD_NOMES_BADGES_QUE_TBM_SAO_TAGS, 
# MAGIC   CASE WHEN a._TagBased = "True" THEN COUNT(a._Name) END AS QTD_TOTAL_NOMES_BADGES_TRUE_TAG_BASED,
# MAGIC   CASE WHEN a._TagBased = "False" THEN COUNT(a._Name) END AS QTD_NOMES_BADGES_FALSE_TAG_BASED 
# MAGIC FROM (SELECT DISTINCT _Name, _TagBased FROM bronze.badges) as a
# MAGIC   LEFT JOIN bronze.tags as b
# MAGIC     ON a._Name = b._TagName
# MAGIC GROUP BY a._TagBased
# MAGIC 
# MAGIC -- Essa análise mostra que a coluna _TagBased tem 100% de relação com a tabela de Tags. Sendo assim, o _Name apresentado na tabela de Badges vem igual ao _TagName exposto na tabela de Tags, quando o _TagBased é "True", e por isso nenhum tratamento de string deve ser realizado nesse campo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a._Name, a._Class, MAX(_Date)
# MAGIC FROM bronze.badges a
# MAGIC   JOIN (SELECT _Name FROM (SELECT _Class, _Name FROM bronze.badges GROUP BY _Class, _Name) GROUP BY _Name HAVING COUNT(_Name) > 1) b
# MAGIC     ON a._Name = b._Name
# MAGIC GROUP BY a._Name, a._Class
# MAGIC ORDER BY a._Name, MAX(_Date)
# MAGIC 
# MAGIC -- Analisando o site "https://stackoverflow.com/help/badges" e a consulta acima podemos inferir que a coluna _Class representa as categorias "bronze", "silver" e "gold".
# MAGIC -- Também podemos inferir que a mesma medalha pode iniciar na bronze e ir "subindo" ou "descendo" conforme passa o tempo
# MAGIC -- _Class = 3 é bronze
# MAGIC -- _Class = 2 é silver
# MAGIC -- _Class = 1 é gold

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT _Name, _Date FROM bronze.badges GROUP BY _Name, _Date
# MAGIC SELECT *
# MAGIC FROM bronze.badges a
# MAGIC   JOIN (SELECT _UserId, _Name, COUNT(1) FROM bronze.badges GROUP BY _UserId, _Name HAVING COUNT(*) > 1 ORDER BY _UserId DESC LIMIT 1) b
# MAGIC     ON a._UserId = b._UserId
# MAGIC     -- AND a._Name = b._Name
# MAGIC ORDER BY _Date
# MAGIC     
# MAGIC -- A coluna _Date poderia ser a data que o usuário ganha a medalha (podendo ganhar a mesma medalha várias vezes e no mesmo momento).

# COMMAND ----------

SELECT _UserId, _Name
FROM (
  SELECT _UserId, _Name, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY _UserId ORDER BY COUNT(1) desc) AS ORDEM
  FROM bronze.badges 
  WHERE _Class = 1 
  GROUP BY _UserId, _Name
  ORDER BY _UserId, ORDEM
) AS a
WHERE ORDEM = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT ID_USUARIO, NOME_MEDALHA, 1 CATEGORIA
# MAGIC FROM (
# MAGIC   SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS ORDEM
# MAGIC   FROM silver.usuario_medalhas 
# MAGIC   WHERE CLASSIFICACAO_MEDALHA = 1 
# MAGIC   GROUP BY ID_USUARIO, NOME_MEDALHA
# MAGIC   ORDER BY ID_USUARIO, ORDEM
# MAGIC ) AS a
# MAGIC WHERE ORDEM = 1
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT ID_USUARIO, NOME_MEDALHA, 2 CATEGORIA
# MAGIC FROM (
# MAGIC   SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS ORDEM
# MAGIC   FROM silver.usuario_medalhas 
# MAGIC   WHERE CLASSIFICACAO_MEDALHA = 2 
# MAGIC   GROUP BY ID_USUARIO, NOME_MEDALHA
# MAGIC   ORDER BY ID_USUARIO, ORDEM
# MAGIC ) AS a
# MAGIC WHERE ORDEM = 1
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT ID_USUARIO, NOME_MEDALHA, 3 CATEGORIA
# MAGIC FROM (
# MAGIC   SELECT ID_USUARIO, NOME_MEDALHA, COUNT(1) AS QTD_VEZES_RECEBIDA, row_number() OVER(PARTITION BY ID_USUARIO ORDER BY COUNT(1) desc) AS ORDEM
# MAGIC   FROM silver.usuario_medalhas 
# MAGIC   WHERE CLASSIFICACAO_MEDALHA = 3 
# MAGIC   GROUP BY ID_USUARIO, NOME_MEDALHA
# MAGIC   ORDER BY ID_USUARIO, ORDEM
# MAGIC ) AS a
# MAGIC WHERE ORDEM = 1
# MAGIC 
# MAGIC ORDER BY ID_USUARIO, CATEGORIA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a,
# MAGIC      b,
# MAGIC      dense_rank() OVER(PARTITION BY a ORDER BY b),
# MAGIC      rank() OVER(PARTITION BY a ORDER BY b),
# MAGIC      row_number() OVER(PARTITION BY a ORDER BY b)
# MAGIC FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _UserId, _Name, _Class, MIN(_Date), MAX(_Date), COUNT(*) 
# MAGIC FROM bronze.badges
# MAGIC GROUP BY _UserId, _Name, _Class
# MAGIC HAVING COUNT(*)>1
# MAGIC order by _UserId, _Name, _Class

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _Name
# MAGIC FROM bronze.badges
# MAGIC GROUP BY _Name
# MAGIC HAVING COUNT(_Class)>1
# MAGIC 
# MAGIC -- Pode existir mais de uma Classificação pra cada Medalha. Por exemplo, a medalha "git" pode ser tanto "bronze", quanto "prata".

# COMMAND ----------

# MAGIC %md # 3. Post History

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.post_history WHERE _PostId = 123 ORDER BY _CreationDate

# COMMAND ----------

# MAGIC %md # 4. Post Links

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.post_links WHERE _PostId = '84263' OR _RelatedPostId = '84263'
# MAGIC 
# MAGIC -- Podemos analisar diretamente no post (https://stackoverflow.com/questions/84263/net-abstract-classes) no canto lateral direito os posts linkados que batem com a query acima.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT _LinkTypeId, COUNT(1) FROM bronze.post_links GROUP BY _LinkTypeId -- 1 (count = 6770162) e 3 (count = 1259192)
# MAGIC SELECT * FROM bronze.post_links where _LinkTypeId = 3

# COMMAND ----------

# MAGIC %md # 5. Posts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.posts WHERE _PostTypeId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _PostTypeId FROM bronze.posts 

# COMMAND ----------

# MAGIC %md # 6. Tags

# COMMAND ----------

df_tags = spark.table("bronze.tags")

# COMMAND ----------

df_tags.printSchema()
display(df_tags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Análise para conhecimento do dataset 

# COMMAND ----------

# DBTITLE 1,Cada linha é uma Tag diferente
if(df_tags.count() == df_tags.select(countDistinct('_TagName')).collect()[0][0]):
    print(True)
else:
    print(False)
    
# count = 62707

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Colunas que não são importantes

# COMMAND ----------

# MAGIC %md
# MAGIC * _ExcerptPostId não sabemos do que se trata
# MAGIC * _WikiPostId é uma coluna que faz referência a uma tabela que não existe na origem de onde pegamos os dados

# COMMAND ----------

# DBTITLE 1,Análise sobre a coluna "_Count"
# MAGIC %sql
# MAGIC -- Essa coluna _Count pode ser a contagem de vezes que essa tag foi utilizada nas Postagens, porém são "levemente" diferentes, por isso para fins de exatidão não iremos utilizá-la.
# MAGIC SELECT 
# MAGIC   (SELECT _Count as count_tabela_tags FROM bronze.tags where _TagName = 'python'),
# MAGIC   (SELECT COUNT(1) as count_tabela_posts FROM bronze.posts WHERE _Tags LIKE '%<python>%')

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.users

# COMMAND ----------

# MAGIC %md # 8. Votes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.votes limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.votes a
# MAGIC   join bronze.posts b
# MAGIC     on a._PostId = b._Id
# MAGIC WHERE _VoteTypeId = 1 -- AND _UserId is not null

# COMMAND ----------

# MAGIC %md # 8. Vote Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.vote_types

# COMMAND ----------

# MAGIC %md # Post Types

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.tipo_postagem

# Databricks notebook source
import xml.etree.ElementTree as ET

# COMMAND ----------

dir_stackoverflow_menor = "/dbfs/mnt/raw/stackoverflow/"
dir_stackoverflow_maior = "/dbfs/mnt/raw/processing/stackoverflow/"
dir_origem = dir_stackoverflow_menor

# COMMAND ----------

# DBTITLE 1,Badges
arq_origem = dir_origem + "Badges.xml" 
et_badges = ET.parse(arq_origem)
root_badges = et_badges.getroot()
print(root_badges.tag)
print(ET.tostring(root_badges, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,Comments
arq_origem = dir_origem + "Comments.xml" 
et_comments = ET.parse(arq_origem)
root_comments = et_comments.getroot()
print(root_comments.tag)
print(ET.tostring(root_comments, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,PostHistory
arq_origem = dir_origem + "PostHistory.xml" 
et_posthistory = ET.parse(arq_origem)
root_posthistory = et_posthistory.getroot()
print(root_posthistory.tag)
print(ET.tostring(root_posthistory, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,PostLinks
arq_origem = dir_origem + "PostLinks.xml" 
et_postlinks = ET.parse(arq_origem)
root_postlinks = et_postlinks.getroot()
print(root_postlinks.tag)
print(ET.tostring(root_postlinks, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,Posts
arq_origem = dir_origem + "Posts.xml" 
et_posts = ET.parse(arq_origem)
root_posts = et_posts.getroot()
print(root_posts.tag)
print(ET.tostring(root_posts, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,Tags
arq_origem = dir_origem + "Tags.xml" 
et_tags = ET.parse(arq_origem)
root_tags = et_tags.getroot()
print(root_tags.tag)
print(ET.tostring(root_tags, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,Users
arq_origem = dir_origem + "Users.xml" 
et_users = ET.parse(arq_origem)
root_users = et_users.getroot()
print(root_users.tag)
print(ET.tostring(root_users, encoding='utf8').decode('utf8'))

# COMMAND ----------

# DBTITLE 1,Votes
arq_origem = dir_origem + "Votes.xml" 
et_votes = ET.parse(arq_origem)
root_votes = et_votes.getroot()
print(root_votes.tag)
print(ET.tostring(root_votes, encoding='utf8').decode('utf8'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Testes

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

lista_arquivos_xml = [
    'Badges',
    'Comments',
    'PostHistory',
    'PostLinks',
    'Posts',
    'Tags',
    'Users',
    'Votes'
]

# COMMAND ----------

for arquivo_xml in lista_arquivos_xml:
    globals()[f"df_{arquivo_xml.lower()}"] = 1

# COMMAND ----------

df_users

# COMMAND ----------

df_badges = spark.read.format("xml").options(rootTag="badges", rowTag="row").load("/mnt/raw/stackoverflow/Badges.xml")
#display(df_badges)

# COMMAND ----------

display(df_badges)

# COMMAND ----------

display(df_badges)
createOrReplaceTempView(df_badges)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_badges

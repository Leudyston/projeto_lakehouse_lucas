# Databricks notebook source
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
  globals()[f"df_{arquivo_xml.lower()}"] = spark.read.format("xml").options(rootTag="badges", rowTag="row").load("/mnt/raw/stackoverflow/Badges.xml")

# COMMAND ----------

df_users

# COMMAND ----------



# Databricks notebook source
import py7zr

# COMMAND ----------

lista_arquivos = [
    # 'Badges',
    # 'Comments',
    'PostHistory',
    'PostLinks',
    'Posts',
    'Tags',
    'Users',
    'Votes'
]

for arquivo in lista_arquivos:
    archive = py7zr.SevenZipFile(f'/dbfs/mnt/raw/landing/stackoverflow.com-{arquivo}.7z', mode='r')
    archive.extractall(path='/dbfs/mnt/raw/processing/stackoverflow/')
    archive.close()

# COMMAND ----------



-- Databricks notebook source
SELECT * FROM bronze.tags where _TagName = 'python'

-- COMMAND ----------

SELECT COUNT(*) FROM bronze.posts WHERE _Tags LIKE '%python%' 

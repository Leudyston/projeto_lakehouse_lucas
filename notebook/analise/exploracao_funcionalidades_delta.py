# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.votes;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE bronze.votes TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.votes

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze.votes;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE bronze.votes TO VERSION AS OF 4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.votes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze.votes

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze.votes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.votes; 

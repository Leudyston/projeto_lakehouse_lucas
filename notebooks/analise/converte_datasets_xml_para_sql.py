# Databricks notebook source
pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper

# COMMAND ----------

import bq_helper
from bq_helper import BigQueryHelper
# https://www.kaggle.com/sohier/introduction-to-the-bq-helper-package
stackOverflow = bq_helper.BigQueryHelper(active_project="bigquery-public-data", dataset_name="stackoverflow")

# COMMAND ----------

import pandas as pd
# https://github.com/SohierDane/BigQuery_Helper
from bq_helper import BigQueryHelper

# COMMAND ----------

bq_assistant = BigQueryHelper("bigquery-public-data", "openaq")

# COMMAND ----------



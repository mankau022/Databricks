# Databricks notebook source
#info = dbutils.fs.ls("dbfs:/FileStore/tables/Country.xlsx")  :gives the information about the file DB
sparkDF = spark.read.format("excel").option("useHeader", "true").option("inferSchema", "true").load("/mnt/lsTest/Country.xlsx")<br>display(sparkDF.collect())
print(sparkDF)

# COMMAND ----------

import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

spark.

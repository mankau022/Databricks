# Databricks notebook source
spark.conf.get('spark.driver.host')

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set('spark.driver.cores',1) #cannot set it on default sparkSession(spark)

# COMMAND ----------


spark.conf.get('spark.driver.cores')

# COMMAND ----------

spark.conf.get('spark.executor.memory')

# COMMAND ----------

spark.conf.get('spark.driver.maxResultSize')

# COMMAND ----------

spark.conf.get('spark.yarn.am.cores')

# COMMAND ----------

spark.conf.get('spark.network.timeout')

# COMMAND ----------

from pyspark.sql import SparkSession
spark1 = SparkSession.builder.config('spark.driver.cores', 10).getOrCreate()

# COMMAND ----------

spark1.conf.get('spark.driver.cores')#here it can be set

# COMMAND ----------

spark1.conf.set('spark.driver.memory',200)

# COMMAND ----------

spark1.conf.get('spark.driver.memory')

# COMMAND ----------



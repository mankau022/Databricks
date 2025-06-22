# Databricks notebook source
df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')
df.show()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save('dbfs:/FileStore/tables/Files/parquet')

# COMMAND ----------

df.write.format('json').mode('overwrite').save('dbfs:/FileStore/tables/Files/json')

# COMMAND ----------

df.write.format('avro').mode('overwrite').save('dbfs:/FileStore/tables/Files/avro')

# COMMAND ----------

df.write.format('orc').mode('overwrite').save('dbfs:/FileStore/tables/Files/orc')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/parquet

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/avro

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/orc

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Evolution

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1 = df.withColumn('additional_col',lit('xyz'))
df1.show(5)

# COMMAND ----------

df1.write.format('parquet').mode('append').save('dbfs:/FileStore/tables/Files/parquet')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/parquet

# COMMAND ----------

df_p = spark.read.format('parquet').load('dbfs:/FileStore/tables/Files/parquet')
df_p.show()
#additional_col is missing although append was successfull.

# COMMAND ----------

df.count(),df_p.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Using mergeSchema for Parquet

# COMMAND ----------

df1.write.format('parquet').option('mergeSchema',True).mode('append').save('dbfs:/FileStore/tables/Files/parquet')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/parquet

# COMMAND ----------

df_p_mS = spark.read.format('parquet').load('dbfs:/FileStore/tables/Files/parquet')
df_p_mS.show()
#additional_col is missing although append with mergeSchema was successfull.

# COMMAND ----------

df_p_mS.count()

# COMMAND ----------

#AVRO
df1.write.format('avro').mode('append').save('dbfs:/FileStore/tables/Files/avro')

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Files/avro

# COMMAND ----------

df_a = spark.read.format('avro').load('dbfs:/FileStore/tables/Files/avro')
df_a.show()
#additional_col is present after append.

# COMMAND ----------

df.count(),df_a.count()

# COMMAND ----------

#df_a.filter(col('additional_col').isNull()).show()
#df_a.filter(col('Car_Name')=='ritz').show()
#df_a.filter("Car_Name=='ritz'").show()
df_a.filter(col('Car_Name').isNull()).show()

# COMMAND ----------



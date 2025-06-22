# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('json').option('multiline',True).option('inserschema',True).load('dbfs:/FileStore/tables/file5.json')

# COMMAND ----------

df.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#if array use explode, if struct use dot(.)
df.select("*",explode_outer(df.restaurants).alias("new_restaurants"))\
  .drop("restaurants")\
  .select('*','new_restaurants.restaurant.R.res_id',
          explode_outer('new_restaurants.restaurant.establishment_types').alias('establishment_types_new'),
          'new_restaurants.restaurant.name').drop("new_restaurants",'code','message','status').show(truncate=False)

# COMMAND ----------

df.show(5)

# COMMAND ----------

df_cache = df.select ("restaurants","results_found")

# COMMAND ----------

df_cache.cache()

# COMMAND ----------

df_cache.show(5)

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',False)
#join without AQE

# COMMAND ----------

df_sales = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/new_sales.txt')

# COMMAND ----------

df_cities = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/cities.txt')

# COMMAND ----------

df_sales.count(), df_cities.count()
#count creates new stage

# COMMAND ----------

df_sales.rdd.getNumPartitions(),df_cities.rdd.getNumPartitions()

# COMMAND ----------

df_sales = df_sales.repartition(16,"city_id")
df_cities = df_cities.repartition(16,"city_id")

# COMMAND ----------

df_sales.cache()
df_cities.cache()

# COMMAND ----------

df_sales_city = df_sales.join(df_cities,df_sales.city_id == df_cities.city_id,"inner")

# COMMAND ----------

df_sales_city.explain()

# COMMAND ----------

df_sales_city.write.format('noop').mode('overwrite').save('dbfs:/Target/Sales_join')
#without AQE it took 1 min to join

# COMMAND ----------

df_sales_p.write.format('delta').mode('overwrite').saveAsTable('Sales')

# COMMAND ----------



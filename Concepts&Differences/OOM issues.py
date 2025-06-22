# Databricks notebook source
spark.conf.get('spark.executor.memory')

# COMMAND ----------

spark.conf.get('spark.driver.memory')

# COMMAND ----------

spark.conf.get('spark.executor.instances')

# COMMAND ----------

spark.conf.get('spark.driver.cores')

# COMMAND ----------

spark.range(100000000).collect()

# COMMAND ----------

spark.conf.set('spark.executor.memory','1g')

# COMMAND ----------

df = spark.range(100000000)

# COMMAND ----------

df.show()

# COMMAND ----------

df.cache()

# COMMAND ----------

df_car = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')

# COMMAND ----------

df_cache = df_car
df_cache.cache()
df_cache.show()

# COMMAND ----------

df_car.show(5)

# COMMAND ----------

df_cache.show(5)

# COMMAND ----------

df_cache.filter(df_cache.Car_Name=='ritz').show()

# COMMAND ----------

df2 = df_cache.orderBy(df_cache.Car_Name)
df2.show()

# COMMAND ----------

df_cache.orderBy(df_cache.Car_Name).explain()

# COMMAND ----------

df_car.show(5)

# COMMAND ----------



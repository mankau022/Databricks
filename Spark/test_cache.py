# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

df =spark.read.option('header','true').csv('dbfs:/FileStore/tables/nyra_tracking_table.csv')
#df.orderBy('program_number').count()

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

print(spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

df.repartition("track_id").write.format("noop").mode("overwrite").save("dbfs:/input_output")

# COMMAND ----------

df.groupBy("race_number").count().show()

# COMMAND ----------

df.groupBy().count().show()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

df_large = df
for  i in range(0,10):
  df_large=df_large.union(df)

# COMMAND ----------

df_large.count()

# COMMAND ----------

df_large.cache()

# COMMAND ----------

df_large.count()
#reading from disc after cache

# COMMAND ----------

df_large.count()
#reading from memory

# COMMAND ----------

df.select(year('race_date')).distinct().show()

# COMMAND ----------

df.withColumn('race_date',regexp_replace('race_date','2019','2020')).show(5)

# COMMAND ----------

df_verylarge = df
for  i in range(0,25):
  if i==22:
    df=df.withColumn('race_date',regexp_replace('race_date','2019','2020'))
  df_verylarge=df_verylarge.union(df)

# COMMAND ----------

df_verylarge.groupBy(year('race_date')).count().show()
#135939180

# COMMAND ----------

df_verylarge.cache()

# COMMAND ----------

df_verylarge.count()
#from disc took 16 min

# COMMAND ----------

df_verylarge.count()
#from cache

# COMMAND ----------

df_verylarge.groupBy(spark_partition_id()).count().show()

# COMMAND ----------

df_verylarge.rdd.getNumPartitions()

# COMMAND ----------

df_verylarge2 = df_verylarge.repartition(year('race_date'))

# COMMAND ----------

df_verylarge2.rdd.getNumPartitions()

# COMMAND ----------

df_verylarge2.show(5)

# COMMAND ----------

df_verylarge2.schema

# COMMAND ----------

df_verylarge2.groupBy(spark_partition_id()).count().show()

# COMMAND ----------

df_verylarge2.write.format('csv').mode('overwrite').save('dbfs:/FileStore/tables/nyra_tracking_large')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the skewed data

# COMMAND ----------

df_sk =spark.read.option('header','true').csv('dbfs:/FileStore/tables/nyra_tracking_large/*csv')

# COMMAND ----------

df_sk.rdd.getNumPartitions()

# COMMAND ----------

df_sk.groupBy(spark_partition_id()).count().show()
#data is evenly distributed although there were 2 files

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')
#disable AQE

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

df_sk_naqe =spark.read.option('header','true').csv('dbfs:/FileStore/tables/nyra_tracking_large/*csv')

# COMMAND ----------

df_sk_naqe.rdd.getNumPartitions()

# COMMAND ----------



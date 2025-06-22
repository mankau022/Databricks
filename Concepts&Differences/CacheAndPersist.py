# Databricks notebook source
# MAGIC %md
# MAGIC https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist/#:~:text=Spark%20Cache%20vs%20Persist&text=Both%20caching%20and%20persisting%20are,the%20user%2Ddefined%20storage%20level.  
# MAGIC https://luminousmen.com/post/explaining-the-mechanics-of-spark-caching  
# MAGIC BroadCast Join  
# MAGIC https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf#:~:text=In%20broadcast%20hash%20join%2C%20copy,associating%20worker%20nodes%20with%20mappers).

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType
from pyspark.storagelevel import StorageLevel 

# COMMAND ----------

#df = spark.read.csv('dbfs:/FileStore/tables/car_data.csv',header=True,inferSchema=True)
df = spark.read.format('csv').option('header',True).option('mode','permissive').load('dbfs:/FileStore/tables/car_data.csv')
df.show(2)
df.printSchema()
#mode='permissive' makes null, 'dropmalformed' removes record, 'failfast'. Option 'badrecordspath' can give path for bad records for future analysis(do not use mode with this opion).

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("Car_Name","Selling_Price").filter(df.Car_Name=='ritz').explain()

# COMMAND ----------

df1 = df.select("Car_Name","Selling_Price").cache()#Disk Memory Deserialized 1x Replicated(MEMORY_AND_DISK) for DF.

# COMMAND ----------

df1.select("Car_Name","Selling_Price").filter(df.Car_Name=='ritz').show(5)

# COMMAND ----------


rdd_ = df.rdd.cache()#Memory Serialized 1x Replicated(MEMORY_ONLY) for rdd.
rdd_.count()
#rdd.unpersist()

# COMMAND ----------

df.unpersist()
#unpersist works for cache also as Spark cache() method in Dataset class internally calls persist() method which in turn uses sparkSession.sharedState.cacheManager.cacheQuery to cache the result set of DataFrame or Dataset.
#First unpersist and then only you can cache for differenct storage level

# COMMAND ----------

df.persist(StorageLevel.DISK_ONLY)#Disk Serialized 1x Replicated
#MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2,MEMORY_AND_DISK_2
#It is worth noting that DISK_ONLY and OFF_HEAP always write data in serialized format.

# COMMAND ----------

df.is_cached

# COMMAND ----------

df.storageLevel

# COMMAND ----------

print(f"number of partitions = {df.rdd.getNumPartitions()}")
df_=df.repartition(5)#change partition here to see coalesce increaing partitions
print(f"number of partitions = {df_.rdd.getNumPartitions()}")#coalesce is using this repartition to increase partitions
df__ = df_.coalesce(1)
print(f"number of partitions = {df__.rdd.getNumPartitions()}")
df___ = df__.coalesce(4)
print(f"number of partitions = {df___.rdd.getNumPartitions()}")#partition increases to 4

# COMMAND ----------

print(f"number of partitions = {df.rdd.getNumPartitions()}")

# COMMAND ----------

df_ = df.coalesce(2)
print(f"number of partitions = {df_.rdd.getNumPartitions()}")

# COMMAND ----------

windowSpec = Window.partitionBy().orderBy('Car_Name')

# COMMAND ----------

df.withColumn('Rank',dense_rank().over(windowSpec)).show()

# COMMAND ----------



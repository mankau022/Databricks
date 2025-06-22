# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import pandas as pd

# COMMAND ----------

df = spark.read.format('csv').option('sep',',').option('header',True).load('/FileStore/tables/car_data.csv')
df.show(3)

# COMMAND ----------

dbutils.fs.rm('dbfs:/DeltaOptimize',recurse=True)

# COMMAND ----------

print(df.rdd.getNumPartitions())
df.repartition(2).write.mode('overwrite').partitionBy('Transmission').format('delta').save('dbfs:/DeltaOptimize')

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize 'dbfs:/DeltaOptimize'
# MAGIC --it creates new file combining data from other files and delta table starts refering new files
# MAGIC --remove non-reference files using VACCUUM command as below

# COMMAND ----------

spark.conf.set("spark.databricks.delta.vacuum.logging.enabled",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum delta.`dbfs:/DeltaOptimize` retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC --SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

#to read individual file withing a delta set above property to false
#dff = spark.read.format('parquet').load('dbfs:/DeltaOptimize/Transmission=Manual/part-00001-e02b7e8d-d6bc-4193-ba02-ee3f0781ab83.c000.snappy.parquet')
#dff.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC --https://docs.databricks.com/sql/language-manual/delta-optimize.html
# MAGIC optimize 'dbfs:/DeltaOptimize';--optimize whole table
# MAGIC optimize 'dbfs:/DeltaOptimize' where Transmission='Manual';--optimize only a subset of data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Z order gets the statistics from transaction log and it co-locates the data
# MAGIC --apply it on high cardinality(more distinct values) column which is used frequently in where clause for example.
# MAGIC --can be used with multiple columns, but effectiveness of locality drops with every additional column.
# MAGIC --not idempotent--> if you do zorder 2nd time, internally the location of data will change(while similar data will remain together). will not have any impact on performance
# MAGIC optimize 'dbfs:/DeltaOptimize'
# MAGIC Zorder by Car_Name--co-locates data

# COMMAND ----------

#data Skipping is automatically present in delta tables
#avoid reading irrelevant data
#with OPTIMIZE and ZORDER BY, dataskipping works even better
#Collects first 32 columns defined in table schema to check statistics by default. dataSkippingNumIndexedCols can be used to change this number. Keep your column in first 32
#collecting statistics on long strings is expensive avoid it by configuring dataSkippingNumIndexedCols


# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/DeltaOptimize`

# COMMAND ----------

#Delta Cache
#Accelerates reads.
#Caches data automatically whenever files are retrieved from a remote location
#it is different from spark cache, they operate on different levels altogether.
#delta cache(automatically enabled but can disable it) is actually on SSD, whenever you are reading or writing it will take the delta cache and locate them on worker node.
#spark cache is in-memory operation, the way you specify it.
#for spark cache you need to define it by df.cache() or df.persist(), at which level you want to cache(manually write it in code).
#both of them are lazilly evaluated, perform an action to use delta cache.

# COMMAND ----------



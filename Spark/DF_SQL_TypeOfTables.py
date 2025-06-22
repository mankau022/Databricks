# Databricks notebook source
#https://docs.databricks.com/delta/index.html#:~:text=All%20tables%20on%20Databricks%20are%20Delta%20tables%20by%20default.,the%20lakehouse%20with%20default%20settings.
#go through this link to deep dive in Delta Tables and DLT.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/car_data.csv',header=True)

# COMMAND ----------

#creates MANAGED table
df.write.saveAsTable('Car_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended Car_managed

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/car_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.car_managed

# COMMAND ----------

df.write.option('path','/Manish').mode('overwrite').saveAsTable('car_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended car_managed

# COMMAND ----------

dbutils.fs.ls('dbfs:/Manish')

# COMMAND ----------

display(spark.read.format('delta').load('dbfs:/Manish'))

# COMMAND ----------

version = spark.sql("select max(version) -1 from (describe history 'dbfs:/Manish')").collect()
print(version)
df = spark.read.format('delta').option('versionAsOf',version[0][0]).load('dbfs:/Manish')
df.show(3)

# COMMAND ----------

from delta.tables import *
deltatable = DeltaTable.forPath(spark,"dbfs:/Manish")
display(deltatable.history())

# COMMAND ----------

#deltatable.restoreToVersion(1)
#deltatable.restoreToTimestamp('2022-12-22T09:36:48.000+0000')

# COMMAND ----------

#reading 1st version of delta table
df = spark.read.format('delta').option('versionAsOf',1).load('dbfs:/Manish')
df.show(3)

# COMMAND ----------

#reading version based on timestamp of delta table
df = spark.read.format('delta').option('timestampAsOf','2022-12-22T09:36:48.000+0000').load('dbfs:/Manish')
df.show(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/Manish` version as of 1 limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC --if you want ro restore previous version as latest
# MAGIC restore table delta.`dbfs:/Manish` to version as of 1
# MAGIC --deltatable.restoreToVersion(1) for python

# COMMAND ----------

display(deltatable.history())

# COMMAND ----------

# run it to capture VACUUM in logs
#spark.conf.set("spark.databricks.delta.vacuum.logging.enabled",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --default retension period is 7days or 168hrs
# MAGIC --set spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC --run above command, else you will get error as we are trying to use vaccum with retention period less than 7 days.
# MAGIC --vacuum delta.`dbfs:/Manish` retain 0 hours --dry run
# MAGIC --dry run will not let remove file instead it will let u kno which files vaccum is going to remove

# COMMAND ----------

dbutils.fs.ls('dbfs:/Manish')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history 'dbfs:/Manish'
# MAGIC --select * from delta.`dbfs:/Manish` version as of 3

# COMMAND ----------

#for python
#deltatable.vacuum(retentionHours=0)

# COMMAND ----------

#mergeSchema
#df.write.format('delta').option('mergeSchema','true').mode('append').save('dbfs:/Manish')
#adapts to schema change

# COMMAND ----------

df.createOrReplaceTempView('car_tempview')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended car_tempview

# COMMAND ----------

df.createOrReplaceGlobalTempView('car_globaltempview')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended global_temp.car_globaltempview

# COMMAND ----------

spark.sql('select * from global_temp.car_globaltempview').show(3)

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended global_temp.car_globaltempview

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table car_fromglobaltempview
# MAGIC using delta
# MAGIC location 'dbfs:/fromTempView'
# MAGIC as
# MAGIC select * from global_temp.car_globaltempview

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.car_globaltempview limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history car_fromglobaltempview

# COMMAND ----------

df.write.format('csv').options(header=True).partitionBy('Fuel_Type').mode('overwrite').save('dbfs:/Somya')
#Save modes: 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'.

# COMMAND ----------

dbutils.fs.ls('dbfs:/Somya')

# COMMAND ----------

#partitionBy column shifted to end.
df_csv = spark.read.csv('dbfs:/Somya',header=True)
df_csv.show(3)

# COMMAND ----------

#returns shape of DF. Pandas: df.shape
print(f"Rows :{df_csv.count()}, columns :{len(df.columns)}")

# COMMAND ----------

df.select('Car_Name','Fuel_Type').withColumn('Fuel_Cat',expr("case Fuel_Type when 'Petrol' then 1 when 'Diesel' then 2 else 3 end")).show(3)

# COMMAND ----------

df.select('Car_Name','Fuel_Type').withColumn('Fuel_Cat',when(col('Fuel_Type')=='Petrol',1).when(col('Fuel_Type')=='Diesel',2).otherwise(3)).show(3)

# COMMAND ----------

df.selectExpr('Car_Name','Fuel_Type',"case Fuel_Type when 'Petrol' then 1 when 'Diesel' then 2 else 3 end as Fuel_Cat").show(3)

# COMMAND ----------



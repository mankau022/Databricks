# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/department_data.txt')

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Autoloaded

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/FileStore/autoloader/source'

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/autoloader/schemaLocation',True)
dbutils.fs.rm('dbfs:/FileStore/autoloader/checkpointLocation',True)

# COMMAND ----------

# MAGIC %fs cp 'dbfs:/FileStore/autoloader/department_data.txt' 'dbfs:/FileStore/autoloader/source/department_data_01.txt' 

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/autoloader/source'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists Table_AL

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
  .option('cloudFiles.format','csv')\
  .option('cloudFiles.schemaLocation','dbfs:/FileStore/autoloader/schemaLocation')\
  .option('header',True)\
  .load('dbfs:/FileStore/autoloader/source').withColumn('Filename',input_file_name())  

# COMMAND ----------

query = df.writeStream.format('delta')\
  .option('checkpointLocation','dbfs:/FileStore/autoloader/checkpointLocation')\
  .option('mergeSchema',True)\
  .trigger(availableNow=True)\
  .outputMode('append')\
  .toTable('Table_AL')

# COMMAND ----------

spark.sql('select * from Table_AL').show(20)

# COMMAND ----------

# MAGIC %fs cp 'dbfs:/FileStore/autoloader/department_data.txt' 'dbfs:/FileStore/autoloader/source/department_data_02.txt' 

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/autoloader/source'

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
  .option('cloudFiles.format','csv')\
  .option('cloudFiles.schemaLocation','dbfs:/FileStore/autoloader/schemaLocation')\
  .option('header',True)\
  .load('dbfs:/FileStore/autoloader/source').withColumn('Filename',input_file_name())  

# COMMAND ----------

query = df.writeStream.format('delta')\
  .option('checkpointLocation','dbfs:/FileStore/autoloader/checkpointLocation')\
  .option('mergeSchema',True)\
  .trigger(availableNow=True)\
  .outputMode('append')\
  .toTable('Table_AL')

# COMMAND ----------

spark.sql('select * from Table_AL').show()
#did not consumed file which is already consumed 

# COMMAND ----------

# MAGIC %md
# MAGIC ###SchemaEvolution

# COMMAND ----------

new_Schema = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/department_data.txt').withColumn('dummyCol',lit('abc'))

# COMMAND ----------

new_Schema.show()

# COMMAND ----------

new_Schema.rdd.getNumPartitions()

# COMMAND ----------

new_Schema.write.format('csv').option('header',True).save('dbfs:/FileStore/autoloader/new_schema')

# COMMAND ----------

file_df = spark.createDataFrame(dbutils.fs.ls('dbfs:/FileStore/autoloader/new_schema'))

# COMMAND ----------

file = file_df.filter(col('name').like('%csv')).collect()[0][0]
# type = pyspark.sql.types.Row

# COMMAND ----------

dbutils.fs.cp(file,'dbfs:/FileStore/autoloader/source/department_data_03.txt')

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
  .option('cloudFiles.format','csv')\
  .option('cloudFiles.schemaLocation','dbfs:/FileStore/autoloader/schemaLocation')\
  .option('header',True)\
  .load('dbfs:/FileStore/autoloader/source').withColumn('Filename',input_file_name())  

# COMMAND ----------

query = df.writeStream.format('delta')\
  .option('checkpointLocation','dbfs:/FileStore/autoloader/checkpointLocation')\
  .option('mergeSchema',True)\
  .trigger(availableNow=True)\
  .outputMode('append')\
  .toTable('Table_AL')
#org.apache.spark.sql.catalyst.util.UnknownFieldException: Encountered unknown field(s) during parsing: [dummyCol] in CSV file: dbfs:/FileStore/autoloader/source/department_data_03.txt
#Fails with above error when schemaChange is detected  
#in next run, it runs successfully

# COMMAND ----------

spark.sql('select count(*) as cnt from Table_AL').show()
#did not consumed file which is already consumed 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Table_AL
# MAGIC --did not consumed file which is already consumed 

# COMMAND ----------



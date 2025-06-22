# Databricks notebook source
# MAGIC %md
# MAGIC ###Structured Streaming  
# MAGIC https://docs.databricks.com/structured-streaming/index.html  
# MAGIC https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

# COMMAND ----------

#stream = spark.readStream.format('rate').option('rowsPerSecond',500).load()
df_c = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')
spark.sql("drop table if exists car_managed ")
dbutils.fs.rm('dbfs:/user/hive/warehouse/car_managed',True)
df_c.write.saveAsTable('car_managed')
df = spark.sql("select * from car_managed")

# COMMAND ----------

dbutils.fs.rm('dbfs:/Stream/DeltaStream',recurse=True)

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('dbfs:/Stream/DeltaStream/full')
df_small = df.limit(2)
df_small.write.format('delta').mode('overwrite').save('dbfs:/Stream/DeltaStream/small')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists car;
# MAGIC drop table if exists car_small;
# MAGIC
# MAGIC create  table default.car
# MAGIC using delta 
# MAGIC location 'dbfs:/Stream/DeltaStream/full';
# MAGIC
# MAGIC create table default.car_small
# MAGIC using delta 
# MAGIC location 'dbfs:/Stream/DeltaStream/small';

# COMMAND ----------

# MAGIC %sql
# MAGIC --run to make changes in table
# MAGIC insert into car_small
# MAGIC select * from car order by rand() limit 10;
# MAGIC
# MAGIC --update car_small
# MAGIC --set Fuel_type='CNG' where Fuel_type='Petrol'

# COMMAND ----------

df_st = spark.readStream.format('delta').option('maxBytesPerTrigger',1).option('ignoreChanges',True).load('dbfs:/Stream/DeltaStream/small')
#option('ignoreChanges',True) ignore changes in previously read stream. Pushes all rows of micro batch to target whose part updated row is.
#option('ignoreDeletes',True)
#option('startingVersion',2)
#option('maxfilespertrigger',3) in every batch it will read 3 files. Ignored when outputMode is once
#option('maxBytesPerTrigger',5)

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import *
df_st = df_st.withColumn('TimeStamp',current_timestamp())

# COMMAND ----------

dbutils.fs.rm('dbfs:/Stream/DeltaStream/Stream_checkpoint',recurse=True)
#remove checkpoint and output to restart stream from begining to adjust data change
#If only checkpoint folder is deleted, query will read all available data(even previously read data). It will again read previously read data.

# COMMAND ----------

# MAGIC %sql
# MAGIC --run to make changes in table
# MAGIC insert into car_small
# MAGIC select * from car order by rand() limit 2;
# MAGIC
# MAGIC --update car_small
# MAGIC --set Fuel_type='CNG' where Fuel_type='Petrol'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  car_small
# MAGIC --update car_small set year=0000 where car_name = 'ritz'
# MAGIC --delete from car_small where car_name = 'amaze'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  car_small

# COMMAND ----------

path = 'dbfs:/Stream/DeltaStream/Stream'
#streamQuery = df_st.select('Year','Fuel_Type',expr("cast(Kms_Driven as int) Kms_Driven"))
streamQuery = (df_st#['Year','Fuel_Type','Kms_Driven'].groupBy(['Year','Fuel_Type']).count()
               .writeStream
               .format('delta')
               .outputMode('append')
               .option('checkpointLocation',f"{path}_checkpoint")
               .trigger(once=True)
               .start(path))
#startOffset and endOffset gives information about version of table(reservoirVersion)
#trigger options: availableNow, once, processingTime and continuous
#continuous does not support delta and is meant for experiment
#outputMode: append, complete and update
#Update mode does not support DeltaDataSource
#Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark.
#Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets
#With Append, if you make any changes to already read data using 'ignoreUpdates', whole micro batch will be appended again to target whose part updated row is. Duplicate rows can occur based on trigger you choose.

# COMMAND ----------

df = spark.read.format('delta').load(path)
df.show(truncate=False)

# COMMAND ----------

streamQuery.recentProgress[0]['batchId']

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`dbfs:/Stream/DeltaStream/small`;
# MAGIC --describe history  delta.`dbfs:/Stream/DeltaStream/Stream`
# MAGIC describe history car_small

# COMMAND ----------

#dbutils.fs.rm('dbfs:/Stream/StreamTest/inputdata/',recurse=True) # removes inputdata folder
#delete files selectively from folder
for i in dbutils.fs.ls('dbfs:/Stream/StreamTest/inputdata'):
  if i[0].endswith('.csv'):
    dbutils.fs.rm(i[0])
    

#using 'in'    
lst_file = dbutils.fs.ls(storage+'/empDF')
lst = []
for file in lst_file:
  if '.parquet' in file.name:
    print(file[1])

# COMMAND ----------

#to stop running streams
count = 1
for s in spark.streams.active:
  s.stop()
  count+=1
print(f"{count} streams were active")

# COMMAND ----------

spark.streams.active

# COMMAND ----------

# MAGIC %md
# MAGIC ###OutputMode: Update

# COMMAND ----------

df = spark.createDataFrame([(1,'Manish',10,'BI'),
                           (2,'Vishal',20,'BE')],schema=['Id','Name','Sal','Dept'])
df.show()

# COMMAND ----------

path = 'dbfs:/Stream/StreamCSV'
df.write.format('csv').option('header',True).save(path)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
schema = StructType([StructField('Id',StringType(),True),
                    StructField('Name',StringType(),True),
                    StructField('Sal',IntegerType(),True),
                    StructField('Dept',StringType(),True)])
df_st_csv = spark.readStream.format('csv').option('maxBytesPerTrigger',1).schema(schema).load(path)

# COMMAND ----------

csv_streamQuery = (df_st_csv
                   .writeStream
                   .format('csv')
                   .outputMode('update')
                   .trigger(once=True)
                   .start('dbfs:/Stream/CSVOutput'))
#Data source csv does not support Update output mode

# COMMAND ----------



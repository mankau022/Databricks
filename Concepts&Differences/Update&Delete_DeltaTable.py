# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# COMMAND ----------

data = ([1,'Manish',200],
        [3,'Somya',300],
        [2,'Naimish',None],
        [6,'Vishal',250],
        [7,'Ram',400])

target_df = spark.createDataFrame(data,schema=['Id','Name','Salary'])
#create target delta table
target_df.write.format('delta').mode('overwrite').saveAsTable('t_target_df')      

# COMMAND ----------

source_data = ([1,'Manish',250],
        [2,'Naimish',150],
        [7,'Ram Sharma',400])

source_df = spark.createDataFrame(source_data,schema=['Id','Name','Salary'])
source_df.write.format('delta').mode('overwrite').saveAsTable('t_source_df')        

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_source_df

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into
# MAGIC t_target_df as t 
# MAGIC using t_source_df as s 
# MAGIC on t.Id=s.Id
# MAGIC when matched then 
# MAGIC update set *
# MAGIC when not matched then Insert *
# MAGIC when not matched by source then delete 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update and delete using merge

# COMMAND ----------


data = ([1,'Manish',200],
        [3,'Somya',300],
        [2,'Naimish',None],
        [6,'Vishal',250],
        [7,'Ram',400])

target_df = spark.createDataFrame(data,schema=['Id','Name','Salary'])
#create target delta table
target_df.write.format('delta').mode('overwrite').saveAsTable('t_target_df')      

# COMMAND ----------

source_data = ([1,'Manish',250],
        [2,'Naimish',150],
        [7,'Ram Sharma',400],
        [9,'Amit',450])

source_df = spark.createDataFrame(source_data,schema=['Id','Name','Salary'])
source_df.write.format('delta').mode('overwrite').saveAsTable('t_source_df')        

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_source_df;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into
# MAGIC t_target_df as t 
# MAGIC using t_source_df as s 
# MAGIC on t.Id=s.Id
# MAGIC --when matched then update set *
# MAGIC when matched then delete
# MAGIC --when not matched then Insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history t_target_df

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended t_target_df

# COMMAND ----------

#%fs ls 'dbfs:/user/hive/warehouse/t_target_df'
len(dbutils.fs.ls('dbfs:/user/hive/warehouse/t_target_df'))

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize t_target_df zorder by (Id)

# COMMAND ----------

len(dbutils.fs.ls('dbfs:/user/hive/warehouse/t_target_df'))
#delta table maintains history because of this these many files are there.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add/Alter column

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table t_target_df add column deptId int;
# MAGIC --Alter table t_target_df add column deptId string;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_target_df;

# COMMAND ----------

# MAGIC %md
# MAGIC ###change datatype of column

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table t_target_df

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table t_target_df alter column deptId TYPE bigint;
# MAGIC --int to string type is not supproted
# MAGIC --string cannot be cast to int. Both are not supported
# MAGIC --int to float is not supported
# MAGIC --int to longint not supported
# MAGIC --all above scenarios were tried and none supported
# MAGIC select * from t_target_df;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table t_target_df;

# COMMAND ----------

#changing datatype of column is not supported in delta table.
#Use below code. workaround is to Re-write data with new datatype 
from pyspark.sql import functions as F
(
    spark.read
    .table("delta_table_name")
    .withColumn(
        "Column_name",
        F.col("Column_name").cast("new_data_type")
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("delta_table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop column

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE t_target_df SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name' --enable this property to drop column in delta table
# MAGIC )
# MAGIC --Databricks Runtime 10.2+ supports dropping columns if you enable Column Mapping mode

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table t_target_df drop column deptId;
# MAGIC select * from t_target_df;

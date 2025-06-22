# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,'Manish',1000),
        (2,'Vishal',2000)]
schema = ['Id','Name','Sal']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

stg_data = [(1,'Manish',1500),
        (3,'Naimish',3000)]
stg_schema = ['Id','Name','Sal']

stg_df = spark.createDataFrame(stg_data,stg_schema)
stg_df.show()

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('ExtractTable')
stg_df.write.format('delta').mode('overwrite').saveAsTable('StageTable')

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('path','dbfs:/Target/SCD2/ExtractTableSaveAsTable').saveAsTable('ExtractTableSaveAsTable')
#option- path will lead to creation of External Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended ExtractTableSaveAsTable

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Target/SCD2/ExtractTable'

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from delta.`dbfs:/Target/SCD2/ExtractTable`
# MAGIC select * from ExtractTable

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ExtractTable add columns isactive Int;
# MAGIC --As it was run earlier, isactive col will remain in delta table because path is same.

# COMMAND ----------

# MAGIC %sql
# MAGIC update ExtractTable set isactive=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ExtractTable

# COMMAND ----------

# MAGIC %sql
# MAGIC update e
# MAGIC set e.isactive=0
# MAGIC from ExtractTable e inner join StageTable s
# MAGIC on s.Id=e.Id
# MAGIC --update with join is not working in delta tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ExtractTable e full outer join StageTable s
# MAGIC on s.Id=e.Id

# COMMAND ----------

# MAGIC %sql
# MAGIC --update
# MAGIC select * from ExtractTable e full outer join StageTable s
# MAGIC on s.Id=e.Id where e.sal<>s.sal;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert
# MAGIC select * from ExtractTable e full outer join StageTable s
# MAGIC on s.Id=e.Id where e.id is null;

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD2 with merge(for update) and Insert

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into ExtractTable as target
# MAGIC using StageTable as source
# MAGIC on target.id=source.id
# MAGIC when matched and target.sal<>source.sal
# MAGIC then update set target.isactive=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from ExtractTable

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ExtractTable
# MAGIC select *,1 from StageTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from ExtractTable

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD2 in single MERGE statement

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from extracttable;

# COMMAND ----------

# MAGIC %sql
# MAGIC select id as mergekey,* from stagetable
# MAGIC union all
# MAGIC select null as mergekey,s.*  from stagetable s inner join ExtractTable e on s.id=e.id
# MAGIC where e.isactive=1 and s.sal<>e.sal

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ExtractTable e
# MAGIC USING (select id as mergekey,* from stagetable
# MAGIC union all
# MAGIC select null as mergekey,s.*  from stagetable s inner join ExtractTable e on s.id=e.id
# MAGIC where e.isactive=1 and s.sal<>e.sal) s
# MAGIC ON e.id=s.mergekey
# MAGIC WHEN MATCHED AND e.isactive=1 AND e.sal<>s.sal 
# MAGIC THEN UPDATE SET e.isactive=0--, enddate=current_timestamp() --if timestamp col is there, can be updated here
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT(Id, Name, Sal, Isactive)
# MAGIC VALUES(s.id, s.name, s.sal, 1)
# MAGIC --Merge in Databricks does not care about uniqueness of values in joining col

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ExtractTable

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delete for full load

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ExtractTable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stagetable

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ExtractTable e
# MAGIC USING (select id as mergekey,* from stagetable
# MAGIC union all
# MAGIC select null as mergekey,s.*  from stagetable s inner join ExtractTable e on s.id=e.id
# MAGIC where e.isactive=1 and s.sal<>e.sal) s
# MAGIC ON e.id=s.mergekey
# MAGIC WHEN MATCHED AND e.isactive=1 AND e.sal<>s.sal 
# MAGIC THEN UPDATE SET e.isactive=0--, enddate=current_timestamp() --if timestamp col is there, can be updated here
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT(Id, Name, Sal, Isactive)
# MAGIC VALUES(s.id, s.name, s.sal, 1)
# MAGIC WHEN NOT MATCHED BY SOURCE 
# MAGIC THEN DELETE
# MAGIC --Merge in Databricks does not care about uniqueness of values in joining col

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ExtractTable

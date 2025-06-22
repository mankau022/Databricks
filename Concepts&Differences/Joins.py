# Databricks notebook source
data = [(2,'Manish','Kaushik',1,1000),
       (1,'Vishal','Kaushik',2,2000),
       (3,'Naimish','Kaushik',1,1500),
       (6,'Somya','Sharma',2,2500),
       (7,'amit','singh',2,1000),
       (4,'Meenakshi','Sharma',3,1500)]

df = spark.createDataFrame(data,schema=['Id','FName','LName','DeptId','Sal'])

deptData = [(1,'IT'),
           (2,'Sales'),
           (3,'Marketing')]
df2 = spark.createDataFrame(deptData,schema=['DeptId','DeptName'])

# COMMAND ----------

df.rdd.getNumPartitions(),df2.rdd.getNumPartitions()

# COMMAND ----------

df_join = df.join(df2,df.DeptId==df2.DeptId,'inner')#DeptId comes 2 times from both tables
df_join.show()

# COMMAND ----------

df_join.rdd.getNumPartitions()

# COMMAND ----------

df.join(df2,['DeptId'],'inner').show()#DeptId is only one time
#df.join(df2,['DeptId']).show()

# COMMAND ----------

df_join = df.join(df2,df.DeptId==df2.DeptId,'leftouter').show()
#df_join = df.join(df2,df.DeptId==df2.DeptId,'left').show()
#df_join = df.join(df2,df.DeptId==df2.DeptId,'fullouter').show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
windowSpec = Window.partitionBy('DeptId')
df.select('Id','FName','LName','DeptId','Sal',max(col('Sal')).over(windowSpec).alias('MaxSal'),min(col('Sal')).over(windowSpec).alias('MinSal')).show()
#withColumn
df.withColumn('MaSal',max(df.Sal).over(windowSpec)).withColumn('minSal',min(df.Sal).over(windowSpec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union

# COMMAND ----------

df.unionByName(df2,allowMissingColumns=True).show()#even if schema is not matching.

# COMMAND ----------

try:
  #print(x)
  raise error
except NameError:
  print('Error occurred')
except error:
  print('Error message 2')

# COMMAND ----------

y = 'manish'
if not type(y) is  int:rked on
  raise TypeError('Type not matching')

# COMMAND ----------

try:
  y = 'manish'
  if not type(y) is  int:
    raise TypeError('Type not matching','do something')
  
except TypeError as e:
  print('In except')
  print(e)
  x,y = e.args
  print(x,y)

# COMMAND ----------

a=1
try:
  if a == 1:
    
    raise exception 
  
except:
  print('failure')
else:
  print('else')
finally:
  print('finally')

# COMMAND ----------



# COMMAND ----------



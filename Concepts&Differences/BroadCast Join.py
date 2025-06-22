# Databricks notebook source
# MAGIC %md
# MAGIC Because of shuffeling a lot of serialization and de-serialization is happening throughout the cluster(lot of load on cluster).  
# MAGIC If any table is of size 10MB(threshold value), it will go with broadcast join by default else will go with SortMergeJoin.

# COMMAND ----------


emp = [(1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
       
       
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
      ]
empColumns = ["emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(emp,schema=empColumns)


dept = [("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
       ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(dept,schema=deptColumns)



# COMMAND ----------

empDF.show()
deptDF.show()

# COMMAND ----------

from pyspark.sql.functions import *
#spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)#disables broadcast
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',30000000)
empDF.join(broadcast(deptDF),empDF.emp_dept_id==deptDF.dept_id,'left').explain()
#empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,'left').explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename part files

# COMMAND ----------

emp.rdd.getNumPartitions()

# COMMAND ----------

emp.write.format('csv').mode('overwrite').option('header','true').save('dbfs:/TestFiles')

# COMMAND ----------

csv_files = [file for file in dbutils.fs.ls('dbfs:/TestFiles') if '0.csv' in str(file)]
csv_files

# COMMAND ----------

#rename using dbutils.fs.cp and dbutils.fs.mv
for i,file in enumerate(csv_files):
  dbutils.fs.cp(file[0],'dbfs:/TestFiles/DataFile'+str(i)+'.csv')

# COMMAND ----------

#spark.read.csv('dbfs:/TestFiles/DataFile*.csv').count()
spark.read.csv('dbfs:/TestFiles/part*.csv',header=True).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename part files using Pandas  
# MAGIC Couldn't read file using pandas(says directory doesn't exists)

# COMMAND ----------

import pandas as pd
try:
  for i,file in enumerate(csv_files):
    print(i)
    #print(file[1])
    print('dbfs:/TestFiles/'+str(file[1]))
    df_pandas = pd.read_csv('dbfs:/TestFiles/'+str(file[1]))
    print(df_pandas.shape)
    df_pandas.to_csv('dbfs:/TestFiles/DataFile'+i+'.csv',index=False)
  
except:
  print('It is not a CSV file')

# COMMAND ----------

dbutils.fs.ls('dbfs:/TestFiles')

# COMMAND ----------

pd.read_csv('/dbfs/TestFiles/part-00000-tid-2017945525381112642-8358cd08-93c6-4e6d-acd7-d7d9bbc6b828-161-1-c000.csv')

# COMMAND ----------

df = pd.read_csv('/dbfs/FileStore/tables/car_data.csv')

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

path = '/dbfs/'
for directory in os.listdir(path):
  print(directory)

# COMMAND ----------

os.listdir('/databricks/driver')

# COMMAND ----------

import os
os.listdir('/dbfs/')

# COMMAND ----------

os.listdir('/dbfs/')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create single file out of multiple part files using Pandas.

# COMMAND ----------

#code is not implemented as I couldn't find a way to read data using pandas(always says directory does not exists but it is there).
#run a loop on list of csv part files, read them one by one and append data to existing file using below code
#check if a file exists or not. if does not exists, write the data of one part file, if exists then append to that file.
#mode: By default mode is ‘w’ which will overwrite the file. Use ‘a’ to append data into the file.
df.to_csv(‘existing.csv’, mode=’a’, index=False, header=True)

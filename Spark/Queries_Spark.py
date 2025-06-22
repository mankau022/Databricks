# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

schema = StructType([StructField('Id',IntegerType(),True),
                    StructField('Name',StringType(),True),
                    StructField('Dept',StringType(),True),
                    StructField('Salary',IntegerType(),True)])
df = spark.read.format('csv').schema(schema).load('dbfs:/FileStore/tables/EmpDept.csv')
df.show()

# COMMAND ----------

windowSpec = Window.partitionBy('Dept').orderBy('Salary')
r=3
df.withColumn('Rk',dense_rank().over(windowSpec)).filter(col('Rk')==r).show()

# COMMAND ----------

s = 'Learn the Python challenge'
l = s.split()
l.count('Python')

# COMMAND ----------

#find the position of first occurence of word in string
i = l.index('Python')
c=0
for word in l[:i]:
  c+=len(word)+1
print(c+1)

# COMMAND ----------

#finds position of word
s.find('Python')

# COMMAND ----------



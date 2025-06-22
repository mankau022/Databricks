# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

#spark.conf.get("spark.memory.offHeap.size")
spark.conf.get("spark.memory.offHeap.enabled")

# COMMAND ----------

data = [('UK',[['Banana','Apple'],['Guava','Mango']],{'capital':'Abc'},'Manish Kaushik'),
       ('China',[['Grapes','Orange'],['Kinniu',None]],{'capital':'Shanghai'},'Vishal Kaushik'),
       ('India',[['Jamun']],{'capital':'Delhi'},'None'),
       ('USA',[],{'capital':'New York'},'Rahul'),
       ('Australia',None,{'capital':'Melbourne'},'Amit Shah')]
'''data = [('UK',[['Banana','Apple'],['Guava','Mango']]),
       ('China',[['Grapes','Orange'],['Kinniu',None]]),
       ('India',[['Jamun']]),
       ('USA',[]),
       ('Australia',None)]
'''

schema = StructType([StructField('Country',StringType(),True),
                   StructField('Fruits',ArrayType(ArrayType(StringType())),True),
                    StructField('Properties',MapType(StringType(),StringType()),True),
                    StructField('Name',StringType(),True)])

df = spark.createDataFrame(data,schema=schema)
df.show(truncate=False)

# COMMAND ----------

df.createOrReplaceTempView('DF_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DF_table

# COMMAND ----------

df1 = df.select('Country',explode('Fruits').alias('Fruits'),'Properties')#Explode ignores null and empty Array
df1.select('Country',explode('Fruits').alias('Fruits'),'Properties').show()#explode(explode('col')) not supported

# COMMAND ----------

df2 = df.select('Country',explode_outer('Fruits').alias('Fruits'),'Properties')#Explode_outer considers null and empty Array and returns null
df2.select('Country',explode_outer('Fruits').alias('Fruits'),'Properties').show()

# COMMAND ----------

df.select('Country',flatten('Fruits').alias('Flatten_Fruits'),'Properties').show(truncate=False)#Flatten converts list of list/Array of Array to single list/Array. Works on Array of Array only.

# COMMAND ----------

df.select('Country','Fruits',explode('Properties')).show()#explode on map/Dictionary type col
#

# COMMAND ----------

df3 = df.select('Country',posexplode('Fruits'),'Properties','Name')
df4 = df3.select('*',posexplode('col')).withColumnRenamed('pos','index')#returns position of Array value
df4.show()

# COMMAND ----------

#index and enumerate does not work for renaming col as you will use withColumnRenamed func which renames based on names.
index=0
for name in df4.columns:
  df4 = df4.withColumnRenamed(name,str(index))
  index+=1
df4.show()

# COMMAND ----------

df4.write.mode('overwrite').saveAsTable('table')

# COMMAND ----------

df6 = df.select('Country',explode_outer('Fruits').alias('Fruits'),'Properties','Name')
df6.show()
#df6.rdd.map(lambda x:(x[0],(' ').join(x[1]),x[2])).toDF(schema).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###map and flatMap

# COMMAND ----------

def splt(name,ind):
  try:
    return name.split(' ')[ind]
  except IndexError:#if there is no index=1 then this will return 'None'
    pass

splt_fun = udf(con)

#df7 = df6.rdd.map(lambda x:(x[0],x[3].split(' '),x[2])).toDF()# works fine
df7 = df6.rdd.map(lambda x:(x[0],splt(x[3],0),splt(x[3],1),x[2])).toDF()# do not use registered udf on rdd. use pyhon functions
df7.show(truncate=False)

# COMMAND ----------

df6.rdd.map(lambda x:(splt(x[3],0),splt(x[3],1))).toDF().show()

# COMMAND ----------

df6.rdd.mapPartitions(lambda x:(x[0],splt(x[3],0),splt(x[3],1),x[2])).toDF()

# COMMAND ----------

df6.rdd.flatMap(lambda x:(x[3].split(' '))).collect()

# COMMAND ----------

df7.rdd.map(lambda x:(x[0],''.join(x[1]),x[2])).toDF().show(truncate=False)

# COMMAND ----------

df8 = df6.coalesce(4)
df8.withColumn('partitionId',spark_partition_id()).groupby('partitionId').agg(count(col('Country')).alias('No. of rows')).show()

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.get('spark.sql.files.maxPartitionBytes')

# COMMAND ----------

dic  = {}
type(dic)

# COMMAND ----------

dic['manish']=1

# COMMAND ----------

print(dic)

# COMMAND ----------

print(dic.items())
print(dic.keys())
print(dic.values())

# COMMAND ----------

for i in dic.items():
  print(i[1])

# COMMAND ----------

df_car = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')
df_car.show(5)

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

windowSpec = Window.partitionBy('Car_Name').orderBy(col('Kms_Driven').desc())
df_car.withColumn('Kms_Driven',col('Kms_Driven').cast('int')).withColumn('rank',dense_rank().over(windowSpec)).show(10)

# COMMAND ----------



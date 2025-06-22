# Databricks notebook source
class Person:
  name = 'Vishal'
  def __init__(self,age):
    self.age=age

  def get(self):
    return self.name,self.age

# COMMAND ----------

info = Person(30)
print(info)
print(info.name)
print(info.age)

# COMMAND ----------

class Manish(Person):
  name='Manish'
  
  def __init__(self,age,name):
    self.age=age  
    #self.name=name

    Person.__init__(self,name)

  def get(self):
    return self.name,Person.name,self.age

# COMMAND ----------

info1 = Manish(31,'Somya')
print(info1.name)
print(info1.age)
print(info1.get())

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('json').option("multiline",True).load('dbfs:/FileStore/tables/Emp.json')
df.show(truncate=False)

# COMMAND ----------

#complex_fields = dict([ for col in df.fields])
df.schema

# COMMAND ----------

if str(df.schema)=="StructType([StructField('Employee', ArrayType(StructType([StructField('Designation', StringType(), True), StructField('attribute', ArrayType(StructType([StructField('Department', ArrayType(StructType([StructField('Code', StringType(), True), StructField('Dept_id', LongType(), True), StructField('dept_flag', LongType(), True), StructField('dept_type', StringType(), True)]), True), True), StructField('Parent_id', LongType(), True), StructField('status_flag', LongType(), True)]), True), True), StructField('emp_id', LongType(), True)]), True), True)])":
  print('1') 
else:
  print(0)

# COMMAND ----------

df1.select(['Expand_col']).withColumn('Emp_Designation',col('Expand_col').Designation).show()

# COMMAND ----------

data = [('Manish',31,10),('Somya',31,15),('Manish',31,5),('Somya',31,10),('Manish',31,None)]
schema = StructType([StructField('Name',StringType(),True),
                      StructField('Age',IntegerType(),True),
                      StructField('Sal',IntegerType(),True)])
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

column = ['Age']
df1 = df.groupBy('Name','Age').agg(sum('Sal').alias('Sum_S'))
df1.show()

# COMMAND ----------

df.createOrReplaceTempView('df_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_table limit 1;

# COMMAND ----------

from pyspark.sql.window import Window
windowSpec = Window.partitionBy(col('Name'),col('Age')).orderBy('Sal')
df.withColumn('RowNum',row_number().over(windowSpec)).show()

# COMMAND ----------

df.orderBy(col('Name').desc(),col('sal').asc_nulls_last()).show()


# COMMAND ----------

#get position of word in string
st = 'I want to learn Python programming'
word = 'python'
l = len(word)
for i in range(0,len(st)-l):
  if word==st[i:i+l].lower():
    print("Location is {0} out of {1}".format(i,len(st)))

# COMMAND ----------


#duplicates using list comprehension
l = [1,2,1,4,5,4,2,7,6,8,9]
l1 = [e for e in l if l.count(e)>1]
l1.sort()
l1

# COMMAND ----------

#duplicates in list
l = [1,2,1,2,4,5,4,2,7,6,8,9]
dic = {}
for i in l:
  if i in dic:
    dic[i] = dic[i]+1
  else:
    dic[i] = 1
dic

max_v = max(dic.values())
print('Max value is {0}'.format(max_v))

for i,j in dic.items():
  if j==max_v:
    print('Key with max occurence is {0}'.format(i)) 



# COMMAND ----------

# MAGIC %md
# MAGIC ##String functions

# COMMAND ----------

st = ' I want to learn Python programming '
print(st.split(' '))
print(st.lower())
print(st.upper())
print(st.count('I'))
print(st.endswith('ing'))
print(st.find('to'))##returns -1 if word does not exists
print(st.index('to'))##fails if word does not exists
print(st.replace('I','We'))
print(st.strip())##ltrim and rtrim
print(st.capitalize())
print(st.title())


# COMMAND ----------

print(st[::-1])

# COMMAND ----------

List = ['I', 'want', 'to', 'learn', 'Python', 'programming']

# COMMAND ----------

l = [1,5,6,3,56,8,2,1,2,34,2,23]
l.sort()
print(l)
l.count(2)
l.reverse()
print(l)
print('After reversing list index of 2 is {0}'.format(l.index(2)))
l.pop(2)
l.remove(2)
print(l)
print(l[::-1])

# COMMAND ----------

t = (1,4,7,3,8,4,2,56,23)
type(t)
print(t.count(4))
print(t.index(4))
t[::-1]

# COMMAND ----------

#dictionary
d = {'a':1,'d':4,'b':10}
d.items()
k=d.pop('a')
print(k)
print(d.keys())
v = d.setdefault('c',10)
print(v)
print(d)
print('Key with max value is {0}'.format(max(d,key=d.get)))

# COMMAND ----------

s = {1,2,3,8,7,6,4,1,4,2}
s.remove(2)
print(s)
print(s.update({4,3}))

# COMMAND ----------

[]

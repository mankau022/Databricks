# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,FloatType,StringType
spark = SparkSession.builder.getOrCreate()
list = [1,2,3,4,5]
df = pd.DataFrame(list)
df_spark = spark.createDataFrame([(6,'manish'),(2,'Vishal'),(5,'Somya'),(4,None),(3,'bulbul'),(1,'Manish'),(7,'Karan')],['Id','Name'])
print(df.head(3))
df_spark.show()


# COMMAND ----------

df_spark.dropna(how='any',thresh=1,subset=('Name')).show()

# COMMAND ----------

print('''mansih\tkaushik''')

# COMMAND ----------

class person:
  def __init__(self,name,age,gender):
    self.name=name
    self.age=age
    self.gender=gender
    
  def print_details(self):
    print('Name: {}, Age: {}, Gender: {}'.format(self.name,self.age,self.gender))
    
Somya = person('Somya',28,'Female')
Somya.print_details()

class child(person):
  def __init__(self,name,age,gender,std):
    self.name=name
    self.age=age
    self.gender=gender
    self.std=std
    #self.gender=gender
  
  def std_f(self):
    print('{} is studing in class {}'.format(self.name,self.std))


# COMMAND ----------

naimish = child('Naimish',1,'male',0)
#naimish.print_details()
naimish.std_f()

# COMMAND ----------

df_spark.filter(F.col('Name')=='manish').show()

# COMMAND ----------

df_spark[['Id','Name']].toPandas().dropna().head()

# COMMAND ----------

#replace a value in col 
df_spark.replace(subset='Name',to_replace='manish',value='man').show()

# COMMAND ----------

df_spark.limit(2).show()

# COMMAND ----------

df_spark.take(3)#returns a list of rows in row objects format

# COMMAND ----------

print(df_spark.head(3))#returns an Array of Row type
print(df_spark.head(3)[0].Name)#can access row values

# COMMAND ----------

df_spark.collect()[0][1]#collect returns all elements of DF as an Array of Row type

# COMMAND ----------

df_spark.repartition(numPartitions=3).show()

# COMMAND ----------

df_spark.persist().show()

# COMMAND ----------

df_spark.rdd.toDF().show()
#rdd to DataFramesw

# COMMAND ----------

df_spark.orderBy(F.lower('Name').asc()).show()
#orderBy in Spark is case sensitive(first orders capital letters)

# COMMAND ----------

df_spark.select(F.lower('Name').alias('Name'),'Id').show()

# COMMAND ----------

import matplotlib.pyplot as plt
a=  df_spark.count()
#plt.bar([1,2,3,4],[500,200,300,400])

# COMMAND ----------

df_spark.groupBy(F.lower('Name').alias('Name')).count().toPandas().plot().bar('Name','count')

# COMMAND ----------

ls = ['my','name','is','Manish','Kaushik']
sc.parallelize(ls).glom().collect()

# COMMAND ----------

# MAGIC %scala
# MAGIC val ds = spark.createDataset(ls)
# MAGIC ds.show()
# MAGIC //giving error need to this. Create a dataset using list

# COMMAND ----------

# MAGIC %scala
# MAGIC val dss = df_spark.toDS()
# MAGIC dss.show()

# COMMAND ----------

spark.sparkContext.parallelize(ls).collect()#create an RDD and showing its content.
##Try to convert it to Dataframe.

# COMMAND ----------

data = [('My',1),('Name',2),('is',3)]
rdd=spark.sparkContext.parallelize(data)
rdd.toDF(['Name','Id']).show()

# COMMAND ----------

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),False), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col,struct,when
updatedDF = df2.withColumn("OtherInfo", 
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)


# COMMAND ----------

#check if a column exists in DF
print(df.schema.fieldNames().index('lastname'))
print('lastname' in df.schema.fieldNames())
print(StructField("gender",StringType(),True) in df.schema)
print('Lastname'.lower() in [col.lower() for col in df.schema.fieldNames()])#case-insensitive 

# COMMAND ----------


data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")

#Arthmetic operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show() 
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()


# COMMAND ----------


emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,'left').show()
#why letouter and not leftinner. Test this in SQL

# COMMAND ----------

df=spark.range(20)
df2=df.select((df.id % 3).alias("key"))
print(df2.sampleBy("key", {0: 0.5, 1: 0.6},0).collect())
#Output: [Row(key=0), Row(key=1), Row(key=1), Row(key=1), Row(key=0), Row(key=1), Row(key=1), Row(key=0), Row(key=1), Row(key=1), Row(key=1)]


# COMMAND ----------

df2.show()

# COMMAND ----------

empDF.filter(empDF.name.like('%o%')).show()

# COMMAND ----------

#cannot use map as pandas(can apply on series), here it has to apply on whole RDD
empDF.rdd.map(lambda x:(x[0],x[1][:1]+x[1][1:].upper())).toDF(['id','Name']).show()
#to concat this DF with main DF horizontally, we need to create a row_number column in both DF and use inner join to combine them

# COMMAND ----------

empDF.withColumn('Upper_Name',F.concat(F.substring(empDF.name,0,1),F.upper(empDF.name.substr(2,10)))).show()

# COMMAND ----------

empDF.withColumn('UpperName',F.expr("upper(Name)")).show()

# COMMAND ----------

udf1 = F.udf(lambda x:x[0].lower()+x[1:].upper())
empDF.withColumn('UpperName',udf1(empDF.name)).show()

# COMMAND ----------

empDF.withColumn('UpperName',F.expr("Name")).show()

# COMMAND ----------



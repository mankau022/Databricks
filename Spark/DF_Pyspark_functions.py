# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.udf import UDFRegistration

# COMMAND ----------

emp = [(1,"Smith",None,"2018","10","M",3000.0),
    (2,"Rose",1,"2010","20","M",4000.0),
    (3,"Williams",1,"2010","10","M",1000.0),
    (4,"Jones",2,"2005","10","F",2000.0),
    (5,"Brown",2,"2010","40","",None),
      (6,"Brown",2,"2010",None,"",None)
      ]
empColumns = ["emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(emp,schema=empColumns)

# COMMAND ----------

empDF.printSchema()

# COMMAND ----------

schema_ = StructType([StructField('Id',LongType(),False),
                   StructField('Emp_Name',StringType(),False),
                   StructField('Manager',IntegerType(),True),
                   StructField('Joining',StringType(),False),
                   StructField('Dept_Id',StringType(),True),
                   StructField('Gender',StringType(),False),
                   StructField('Salary',FloatType(),True)])
emp = spark.createDataFrame(empDF.rdd,schema=schema_)

# COMMAND ----------

emp.printSchema()

# COMMAND ----------

emp.show()

# COMMAND ----------

emp=emp.fillna(0)#replaces null in numerical type col only
#emp.replace(subset=['Manager'],to_replace=1,value=0).show()# cannot replace None with this

# COMMAND ----------

emp = emp.withColumn('Salary_Cat',when(col('Salary')>=3000,'High').\
              when((emp['Salary']<3000) & (emp['Salary']>=2000),'Medium').\
              when(col('Salary')<2000,'Low').
              otherwise(emp['Salary']))
emp.show()

# COMMAND ----------

emp.groupBy('Emp_Name').agg(count('Emp_Name').alias('Number of Emp')).show()
#emp.groupBy('Emp_Name').count().withColumnRenamed('count','Number of Emp').show()

# COMMAND ----------

emp.select(col('Emp_Name').alias('Name'),'Id').distinct().show()

# COMMAND ----------

emp.drop_duplicates(['Emp_Name']).show()
#emp.dropDuplicates(['Emp_name']).show() #both are same
#sorts data based on column

# COMMAND ----------

emp.drop('Salary_cat').show()

# COMMAND ----------

emp.dropna(how='any',thresh=2,subset=['Dept_Id','Salary']).show()

# COMMAND ----------

emp.orderBy(col('Joining').desc(),col('Manager').asc(),col('Salary').desc()).show()

# COMMAND ----------

window_spec = Window.partitionBy('Emp_Name').orderBy('Id')
emp.withColumn('Rank',rank().over(window_spec)).show()
emp.withColumn('Row_Num',dense_rank().over(window_spec)).show()
emp.withColumn('Row_Num',row_number().over(window_spec)).show()
#tile is not present

# COMMAND ----------

dept = [(10,'BI'),(40,'Back-end')]
dept = spark.createDataFrame(dept,['Dept','Dept_Name'])
emp.join(dept,on=emp.Dept_Id==dept.Dept,how='left').show()

# COMMAND ----------

data = [(1,'manish,vishal'),(2,'Somya,Meenakshi'),(3,'naimish')]
data = spark.createDataFrame(data,schema=['Id','Name'])
data.select(col('Id').cast(IntegerType()),explode(split(col('Name'),',')).alias('Name')).show()
#use Split and explode together for string_split.
#split: returns an array based on delimeter. 'manish,vishal'->['manish','vishal']
#explode: breaks array into individual row

# COMMAND ----------

#data.rdd.map()
data1= data.select(col('Id').cast(IntegerType()),explode(split('Name',',')).alias('Name'))
data1 = data1.rdd.map(lambda x:(x[0],x[1].capitalize()))
data1 = data1.toDF(['Id','Name'])
data1.show()

# COMMAND ----------

data2 = data.rdd.map(lambda x:(x[0],x[1].split(',')))
data2.toDF().show()

# COMMAND ----------

data2 = data.rdd.flatMap(lambda x:(x.Id,x.Name.split(',')))
#data2.collect()
#data2.toDF(['Name']).show() #Can not infer schema for type
spark.createDataFrame(data2,StringType()).show() #above statement is failing that is whyused Createdataframe
#map has same no. of inputs and outputs whereas flatmap flattens the output(has rows equal or more than input)

# COMMAND ----------

len(data1.columns),data1.count()

# COMMAND ----------

#Merge 2 dataframes horizontally
'''from pyspark.sql.functions import monotonically_increasing_id
customers_df = customers_df.withColumn("id",monotonically_increasing_id() )
customers_df.show()
orders_df = orders_df.withColumn( "id", monotonically_increasing_id() )
orders_df.show()
horiztnlcombined_data = customers_df.join(orders_df,customers_df.id == orders_df.id, how='inner')
horiztnlcombined_data.show()
'''

# COMMAND ----------

data.dtypes

# COMMAND ----------

import requests
api_url = "https://jsonplaceholder.typicode.com/todos/1"
response = requests.get(api_url)
response.json()

# COMMAND ----------

#Set Operators
#emp.exceptAll(emp.filter(col('Dept_Id')<=20)).show()#same as subtract
#emp.intersect(emp.filter(col('Dept_Id')<=20)).show()
#emp.intersectAll(emp.filter(col('Dept_Id')<=20)).show()
#emp.union(emp.filter(col('Dept_Id')<=20)).show()
#emp.unionAll(emp.filter(col('Dept_Id')<=20)).show()
emp.subtract(emp.filter(col('Dept_Id')<=20)).show()

# COMMAND ----------

print(emp.dtypes)

# COMMAND ----------



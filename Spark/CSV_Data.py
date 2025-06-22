# Databricks notebook source
import pandas as pd
#info = dbutils.fs.ls("dbfs:/FileStore/tables/Data.csv")
df = spark.read.csv('/FileStore/tables/Data.csv', header =True, escape = ',')
#cases = spark.read.load("/home/rahul/projects/sparkdf/coronavirusdataset/Case.csv",format="csv", sep=",", inferSchema="true", header="true")
df.orderBy(df.FName.asc()).show()
'''dict = {'name':["aparna", "pankaj", "sudhir", "Geeku"],
        'degree': ["MBA", "BCA", "M.Tech", "MBA"],
        'score':[90, 40, 80, 98]}
df_pan = pd.DataFrame(dict)
df_pan=df.toPandas()
print(df_pan['FName'])
df_spa = spark.createDataFrame(df_pan)
df_spa.show()'''
df_pan = df.toPandas()
df_dict = df_pan[df_pan['FName']=='Manish'].to_dict(orient='records')
for obj in df_dict:
  print(obj['FName'])

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

schema = StructType([\
                  StructField('Name',StructType([StructField('FName',StringType(),False)\
                              ,StructField('LName',StringType(),True)]))\
                    ,StructField('Age',IntegerType(),True)\
                    ,StructField('Dept',StringType(),True)\
                    ,StructField('Salary',FloatType(),True)\
                    ,StructField('Company',StringType(),True)])
data = [(['Manish','Kaushik'],28,'bi',20000.0,'Cybage'),\
       (['Vishal','Kaushik'],27,'SD',22000.0,'Iris'),\
       (['Somya','Sharma'],27,'T',None,'Infosys'),\
       (['Meenakshi','Sharma'],27,'Testing',21000.0,None)]

st_df = spark.createDataFrame(data=data,schema=schema)
st_df.show()

# COMMAND ----------

st_df.printSchema()

# COMMAND ----------

st_df.filter(st_df.Name.FName=='Manish').show()

# COMMAND ----------

st_df.drop('Age','Dept').show()

# COMMAND ----------

st_df.fillna(value=0).show()#fill numeric in int/float type column and 
st_df.fillna('NA').show()#fill string in string type column and 
st_df.fillna('NA',subset=['Company','Salary']).show()#will not replace null in Salary as it is float type

# COMMAND ----------

st_df.distinct().show()#complete row should have duplicate
st_df.dropDuplicates(subset=['Age']).show()#

# COMMAND ----------



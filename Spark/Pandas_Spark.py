# Databricks notebook source
import pandas as pd
import pyspark
from pyspark.sql import functions as F
#info = dbutils.fs.ls("dbfs:/FileStore/tables/Data.csv")
df = spark.read.csv('/FileStore/tables/Data.csv', header =True, escape = ',')
#cases = spark.read.load("/home/rahul/projects/sparkdf/coronavirusdataset/Case.csv",format="csv", sep=",", inferSchema="true", header="true")
df.orderBy(df.FName.asc()).show()



# COMMAND ----------

dfP=df.toPandas()
dfPy=spark.createDataFrame(dfP)
##dfPy.printSchema()
dfPy.show(truncate=False) ##shows data without truncation
dselect = dfPy.select("FName","Lname","Salary")
dselect.show(truncate=False)

# COMMAND ----------

#change data type od columns
dfcol = df.withColumn("EmpSal",F.col("salary").cast("Integer"))
#dfcol.printSchema() datatype changed 
dfcol.show(truncate=False)
dfUcol = dfcol.withColumn('EmpSal',F.col('EmpSal')*1.2) # datatype for new col is double
dfUcol.show(truncate=False)
#dfUcol.printSchema()
dfRN = dfUcol.withColumnRenamed('FName','FirstName').withColumnRenamed('LName','LastName')
dfRN.show()


# COMMAND ----------

#filter and where
dffilter = df.filter(df.Salary=='100')   #filter
dffilter.show(truncate=False)
lis = ['200','250','300']
dffil = df.filter(df.Salary.isin(lis))   #isin
dffil.show(truncate=False)

# COMMAND ----------

dfd = df.distinct().count()      #dropDuplicates()
print(dfd)
dfs = df.sort(df.Salary.desc())  #orderBy() can also be used
dfs.createOrReplaceTempView("Emp")
spark.sql('select FName from Emp').show()

# COMMAND ----------

#groupBy
dfcol = df.withColumn("EmpSal",F.col("salary").cast("Integer"))
dfg = dfcol.groupBy('FName','Lname').sum('EmpSal').alias()
dfg.show()

# COMMAND ----------

#find how to pass values in  isin with col
li = ['100']
df_filter = df.filter(F.col('Salary').isin(li))--
df_filter.show()

# COMMAND ----------

pand = df.toPandas()
ind=len(pand.index)
print(ind)
name=pand[pand['FName']=='Manish'].to_dict(orient='records')
print(name)
for i in name:
  print(i['FName'])
 


# COMMAND ----------

row  = pand.iloc[2]
print(row)


# COMMAND ----------

import pyspark
sal  = pand.where(pand['Salary']>'200').dropna()
print(sal)
ps = spark.createDataFrame(sal)
ps.show()

# COMMAND ----------

sal  = pand.where(pand['FName']=='Manish')
print(sal)
ps = spark.createDataFrame(sal)
ps.show()

# COMMAND ----------

#Pandas
FName='Manish'
name='Manish'
dfPandas = df.toPandas()
dfPandas = dfPandas[dfPandas[FName]==name].iloc[0,:].to_dict()

print(dfPandas)

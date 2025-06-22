# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType

# COMMAND ----------

#df = spark.read.csv('dbfs:/FileStore/tables/car_data.csv',header=True,inferSchema=True)
df = spark.read.format('csv').option('header',True).option('mode','permissive').load('dbfs:/FileStore/tables/car_data.csv')
df.show(2)
df.printSchema()
#mode='permissive' makes null, 'dropmalformed' removes record, 'failfast'. Option 'badrecordspath' can give path for bad records for future analysis(do not use mode with this opion).

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(4)

# COMMAND ----------

df.groupBy('Car_Name').sum('Selling_Price').alias('Sum').show(4)
#alias did not work

# COMMAND ----------

#groupBy 2 columns and order by 2 columns in ascending order
df.groupBy(['Car_Name','Fuel_Type']).agg(mean('Selling_Price').alias('Mean Price')).orderBy(['Car_Name','Fuel_Type'],ascending=True).show(4)

# COMMAND ----------

#groupBy and alias to aggregated column
df_gp = df.groupBy(['Car_Name','Fuel_Type']).agg(mean('Selling_Price').alias('Mean Price'),mean('Kms_Driven').alias('Mean Kms Driver'))
df_gp.show(4,truncate=False)
#df.orderBy(col("department").asc,col("state").asc).show(false)

# COMMAND ----------

df.filter(df['Car_Name']=='Royal Enfield Thunder 350').show(truncate=False)
#Avg of Kms_Driven of 'Royal Enfield Thunder 350' is matching with above results which is  12150

# COMMAND ----------

#Order By multiple columns
df.select(['Car_Name','Fuel_Type','Year']).orderBy(col('Car_Name').desc(),col('Fuel_Type').asc_nulls_first()).show(7)
#sort() is more efficient compared to orderBy() because the data is sorted on each partition individually and this is why the order in the output data is not guaranteed.  On the other hand, orderBy() collects all the data into a single executor and then sorts them. This means that the order of the output data is guaranteed but this is probably a very costly operation.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.select(['Car_Name','Fuel_Type']).distinct().show(5)

# COMMAND ----------

df.groupBy('Car_Name').agg(count('Car_Name').alias('Car Count')).show(6,truncate=False)

# COMMAND ----------

windowSpec = Window.partitionBy().orderBy('Car_Name')
df.select('Car_Name').withColumn('Rank',ntile(100).over(windowSpec)).show(301)
#rank, dense_rank, row_number, lag, lead
#use count of rows in show else it will apply ntile on limited rows instead of whole dataframe

# COMMAND ----------

windowS = Window.partitionBy('Fuel_Type','Car_Name').orderBy('Car_Name')
df.select(['Car_Name','Fuel_Type','Kms_Driven']).withColumn('Lag',lag('Kms_Driven',1).over(windowS)).show()

# COMMAND ----------

df_g = df.groupBy(['Car_Name','Fuel_Type']).agg(mean('Selling_Price').cast(FloatType()).alias('Mean Price'),\
                                                mean('Kms_Driven').cast('int').alias('Mean Kms Driver'))
df_g.show(4,truncate=False)

# COMMAND ----------

df.withColumn('Car',col('Car_Name').cast(StringType())).drop('Car_Name').show(2)

# COMMAND ----------

#In selectexpr we can use SQl like expressions 
df.selectExpr('Car_Name','Cast(Year as Float) as Yr').show(3)

# COMMAND ----------

#createOrReplaceTempView, createOrReplaceGlobalTempView
df.createOrReplaceTempView('CarData')
spark.sql("select * from CarData where Car_Name=='ritz'").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema Analysis;
# MAGIC CREATE TABLE Analysis.Car USING CSV LOCATION 'dbfs:/FileStore/tables/car_data.csv';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended Analysis.Car

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists Car

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Analysis.Car 

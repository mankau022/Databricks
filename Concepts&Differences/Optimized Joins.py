# Databricks notebook source
# MAGIC %fs mkdirs 'dbfs:/Source_Dataset'

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob date, email string, phone string, salary bigint, department_id int"
df_emp = spark.read.format('csv').option('header','true').schema(_schema).load('dbfs:/FileStore/employee_records.txt')
#option('InferSchema','true')
#no Job created to read schema when schema is provided while reading data

# COMMAND ----------

len(df_emp.columns), df_emp.count()

# COMMAND ----------

df_dept = spark.read.format('csv').option('header','true').option('inferSchema','true').load('dbfs:/FileStore/department_data.txt')
#2 Jobs created for schema and inderSchema

# COMMAND ----------

len(df_dept.columns),df_dept.count()

# COMMAND ----------

df_emp.show(3)
df_dept.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SortMergeJoin between Big and Small dataset

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',False)
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

# COMMAND ----------

df_emp_dept = df_emp.join(df_dept,['department_id'],"inner")

# COMMAND ----------

df_emp_dept.write.format('noop').mode('overwrite').save('dbfs:/Target/Employee_join')

# COMMAND ----------

# MAGIC %md
# MAGIC BroadCast Join

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold',10485760)

# COMMAND ----------

df_emp_dept = df_emp.join(df_dept,df_emp.department_id==df_dept.department_id,"inner")

# COMMAND ----------

df_emp_dept.write.format('noop').mode('overwrite').save('dbfs:/Target/Employee_join')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join 2 Big Datasets

# COMMAND ----------

df_sales = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/new_sales.txt')

# COMMAND ----------

df_sales.count()

# COMMAND ----------

df_cities = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/cities.txt')

# COMMAND ----------

df_cities.count()

# COMMAND ----------

df_sales.show(3)
df_cities.show(3)

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',False)
#join without AQE

# COMMAND ----------

df_sales_city = df_sales.join(df_cities,df_sales.city_id == df_cities.city_id,"inner")

# COMMAND ----------

df_sales_city.explain()

# COMMAND ----------

df_sales_city.write.format('noop').mode('overwrite').save('dbfs:/Target/Sales_join')
#without AQE it took 54 seconds to join

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',True)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',True)

# COMMAND ----------

df_sales_city.write.format('noop').mode('overwrite').save('dbfs:/Target/Sales_join')
#took 45 sec with AQE

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Broadcast join using big table

# COMMAND ----------

df_sales_city = df_sales.join(broadcast(df_cities),df_sales.city_id == df_cities.city_id,"inner")

# COMMAND ----------

df_sales_city.write.format('noop').mode('overwrite').save('dbfs:/Target/Sales_join')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Bucketing to improve performance

# COMMAND ----------

df_sales.write.format('csv').mode('overwrite').bucketBy(4,'city_id').option('header','true')\
  .option('path','dbfs:/Target/Sales').saveAsTable('Sales_Table')

# COMMAND ----------

df_cities.write.format('csv').mode('overwrite').bucketBy(4,'city_id').option('header','true')\
  .option('path','dbfs:/Target/Cities').saveAsTable('City_Table')

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

sales_bucket = spark.read.table('sales_table')
city_bucket = spark.read.table('city_table')

# COMMAND ----------

df_joined_bucket = sales_bucket.join(city_bucket,sales_bucket.city_id==city_bucket.city_id,'inner')

# COMMAND ----------

df_joined_bucket.write.format('noop').mode('overwrite').save('dbfs:/Target/Sales_join')
#took 36 with bucketed data

# COMMAND ----------

spark.conf.get('spark.executor.memory')

# COMMAND ----------



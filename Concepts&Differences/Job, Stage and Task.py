# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob date, email string, phone string, salary bigint, department_id int"
df_emp = spark.read.format('csv').option('header','true').schema(_schema).load('dbfs:/FileStore/employee_records.txt')
#option('InferSchema','true')
#no Job created to read schema when schema is provided while reading data

# COMMAND ----------

df_dept = spark.read.format('csv').option('header','true').option('inferSchema','true').load('dbfs:/FileStore/department_data.txt')
#2 Jobs created for schema and inderSchema

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled',False)
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled',False)
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

# COMMAND ----------

df_emp_dept = df_emp.join(df_dept,['department_id'],"inner")

# COMMAND ----------

df_emp_dept.write.format('parquet').mode('overwrite').save('dbfs:/Target/Employee_join')
#1 million rows in output
#168 MB in csv format
#35 MB in Parquet format
#64 MB in AVRO format
#34 MB in ORC format

# COMMAND ----------

# MAGIC %fs ls dbfs:/Target/Employee_join/

# COMMAND ----------

df_emp_dept.groupBy(spark_partition_id()).count().orderBy(col("count").desc()).show()
#check why 4 stages are created. It should have been 5 stages

# COMMAND ----------

df_emp_dept.groupBy('first_name').count().orderBy(col("count").desc()).show()

# COMMAND ----------

df_sort = df_emp_dept.orderBy(col("first_name").desc())
df_sort.write.format('noop').mode('overwrite').save()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Another Example

# COMMAND ----------

df = spark.read.format('csv').option('header',True).option('inferschema',True).load('dbfs:/FileStore/tables/car_data.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------

df = df.repartition(2)

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')
df = df.repartition(2)
df = df.select('car_Name','Year').filter(df.Year>'2014').groupBy('Year').count()
df.collect()

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data = [('Manish','manishkau@cybage.com'),
        ('Vishal','Vishalkau@cybage.com'),
        ('Somya','SomyaSha@infy.com'),
        ('Naimish','NaimishKau@gmail.com')]

schema = ['Name','Email']

# COMMAND ----------

df = spark.createDataFrame(data,schema=schema)
df.show()

# COMMAND ----------

df_c = df.filter(~(df.Email.like('%@cybage.com')))
df_nc = df.filter(df.Email.like('%@cybage.com'))
df_c.show()
df_nc.show()

# COMMAND ----------

df_nc = df_nc.withColumn('Salary',round(rand(55)*10000,0))
#if above column is not added df_nc.schema == df_c.schema is matching.

# COMMAND ----------

df_c.show()
df_nc.show()

# COMMAND ----------

if df_nc.schema == df_c.schema:
  print('Schema Matching')
else:
  print('Schema not Matching')

# COMMAND ----------

df_c.write.format('parquet').mode('overwrite').save('dbfs:/Target/Schema/Parquet')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Target/Schema/Parquet'

# COMMAND ----------

df_nc.write.format('parquet').mode('append').save('dbfs:/Target/Schema/Parquet')

# COMMAND ----------

spark.read.format('parquet').load('dbfs:/Target/Schema/Parquet').show()
#supporing schema evolution but schema enforcement is not there.

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Target/Schema/Parquet'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Enforcement and Evolution in Delta

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/Target/Schema/Delta'

# COMMAND ----------

df_c.write.format('delta').mode('overwrite').save('dbfs:/Target/Schema/Delta')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Target/Schema/Delta'

# COMMAND ----------

spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")

# COMMAND ----------

df_nc.write.format('delta').mode('append').save('dbfs:/Target/Schema/Delta')
#Schema Enforcement: without mergeSchema, it will not allow you to add new rows

# COMMAND ----------

df_nc.write.format('delta').mode('append').option('mergeSchema',True).save('dbfs:/Target/Schema/Delta')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Target/Schema/Delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from delta.`dbfs:/Target/Schema/Delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/Target/Schema/Delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(version) from (describe history delta.`dbfs:/Target/Schema/Delta`);
# MAGIC select max(version)-1 from (describe history delta.`dbfs:/Target/Schema/Delta`);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended delta.`dbfs:/Target/Schema/Delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table delta.`dbfs:/Target/Schema/Delta`

# COMMAND ----------



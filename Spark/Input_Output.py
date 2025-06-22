# Databricks notebook source
# MAGIC %md
# MAGIC DataFrameReader options  
# MAGIC https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameReader.html  
# MAGIC https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameWriter.html

# COMMAND ----------

# MAGIC %md
# MAGIC --> Cell 6: For Round-Robin and Hash partitions  
# MAGIC --> performance of bucketBy in Joins

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

emp = [(1,"Smith",None,"2018","10","M",3000.0),
    (2,"Rose",1,"2010","20","M",4000.0),
    (3,"Williams",1,"2010","10","M",1000.0),
    (4,"Jones",2,"2005","10","F",2000.0),
    (5,"Brown",2,"2010","40","",None),
    (6,"Brown",2,"ten","10","",None),
    (7,"Manish",1,"2011","10","M",1500.0),
    (8,"Vishal",2,"2009","10","F",2000.0),
    (9,"Naimish",2,"2011","40","",3000.0),
    (10,"Manish",2,"two",None,"",None)
      ]
empColumns = ["emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(emp,schema=empColumns)

# COMMAND ----------

empDF.rdd.getNumPartitions()

# COMMAND ----------

#empDF = empDF.coalesce(2).explain(extended=True)
empDF = empDF.coalesce(1)

# COMMAND ----------

#Using repartition you can create hash and round-robin partitions.
#empDF = empDF.repartition(2).explain(extended=True)#RoundRobinPartitioning
#empDF = empDF.repartition(2,col('emp_id')).explain(extended=True)#hashpartitioning
#empDF = empDF.repartitionByRange(2,col('emp_id')).explain(extended=True)#rangepartitioning

# COMMAND ----------

empDF.write.format('csv').mode('overwrite').option('header',True).option('maxRecordsPerFile',5).save('/input_output/emp')

# COMMAND ----------

schema = StructType([StructField('Id',IntegerType(),False),
                    StructField('Name',StringType(),False),
                    StructField('Manager_Id',IntegerType(),False),
                    StructField('Joining',IntegerType(),False),
                    StructField('Dept_Id',IntegerType(),True),
                    StructField('Gender',StringType(),False),
                    StructField('Salary',FloatType(),False)])

# COMMAND ----------

emp = spark.read.format('csv').schema(schema).option('mode','Permissive').option('header',True).option('inferSchema',True).load('dbfs:/input_output/emp')
emp.show()#There is 'ten'(bad Record) in Joining column which is IntegerType()
#default mode is Permissive: converts badvalue to null
#dropMalformed: drops bad records 
#failFast: fails reading 

# COMMAND ----------

emp.rdd.getNumPartitions()#Number of partitions is equal to no. of files it has read.

# COMMAND ----------

emp=spark.read.format('csv').schema(schema).option('badRecordsPath','dbfs:/input_output/emp/badRecords').option('header',True).option('inferSchema',True).load('dbfs:/input_output/emp')
emp.show()
#badRecordsPath directs the bad record to a path in json format

# COMMAND ----------

spark.read.format('json').option("recursiveFileLookup",True).load('dbfs:/input_output/emp/badRecords/').show(truncate=False)
#read bad record from json file
#recursiveFileLookup: reads files from nested folders

# COMMAND ----------

emp.write.format('parquet').partitionBy('Dept_Id').mode('overwrite').saveAsTable('emp_partition')
#as repartition is 2 in cell 11, PartitionBy will create same no. of files(2) for each folder.
#for Nulls, partitionBy will create a separate folder dept_Id=_Hive_Default_Partition_

# COMMAND ----------

# MAGIC %sql
# MAGIC --describe table extended emp_partition
# MAGIC select * from emp_partition

# COMMAND ----------

emp.write.format('parquet').bucketBy(2,'Dept_Id').mode('overwrite').saveAsTable('emp_bucket')
#as repartition=2 in cell 11, for each bucket there will be same no. of files i.e; (repartition*bucket)

# COMMAND ----------

emp.write.format('parquet').partitionBy('Dept_Id').bucketBy(2,'Joining').mode('overwrite').saveAsTable('emp_bucket_Partition')
#cannot use same column for both partitionBy and bucketBy
#Each folder will have (repartition*bucket)=2*2=4 no. of files

# COMMAND ----------

emp.write.format('avro').bucketBy(2,'Dept_Id').mode('overwrite').saveAsTable('emp_bucket_csv')
#managed tables can be saved as avro, csv, orc, json also.
#text format needs columns to be in string format. fails if there is int datatype.

# COMMAND ----------

# MAGIC %sql
# MAGIC --describe table extended emp_bucket_csv
# MAGIC select * from emp_bucket_csv limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test performance of bucketBy  
# MAGIC Try big dataset  
# MAGIC BucketBy: https://towardsdatascience.com/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53  
# MAGIC Joins: https://towardsdatascience.com/about-joins-in-spark-3-0-1e0ea083ea86

# COMMAND ----------

emp.rdd.getNumPartitions()

# COMMAND ----------

emp = emp.coalesce(1)

# COMMAND ----------

schema_car = StructType([StructField('Car_Name',StringType(),True),
                        StructField('Year',IntegerType(),True),
                        StructField('Selling_Price',FloatType(),True),
                        StructField('Present_Price',FloatType(),True),
                        StructField('Kms_Driven',IntegerType(),True),
                        StructField('Fuel_Type',StringType(),True),
                        StructField('Seller_Type',StringType(),True),
                        StructField('Transmission',StringType(),True),
                        StructField('Owner',ByteType(),True)
                        ])
car = spark.read.format('csv').schema(schema_car).option('header',True).load('dbfs:/FileStore/tables/car_data.csv')
car.show(3)
#car.count()

# COMMAND ----------

car.printSchema()

# COMMAND ----------

car.rdd.getNumPartitions()#1 as there is only 1 file

# COMMAND ----------

#car.write.format('parquet').mode('overwrite').saveAsTable('car_parquet_bucket')
#car.write.format('csv').option('header',True).mode('overwrite').saveAsTable('car_csv_bucket')
car.write.format('parquet').bucketBy(3,'Year').sortBy('Year').mode('overwrite').saveAsTable('car_parquet_bucket')
car.write.format('csv').option('header',True).bucketBy(3,'Year').sortBy('Year').mode('overwrite').saveAsTable('car_csv_bucket')

# COMMAND ----------

car_p = spark.read.format('parquet').load('dbfs:/user/hive/warehouse/car_parquet_bucket')
car_c = spark.read.format('csv').option('header',True).load('dbfs:/user/hive/warehouse/car_csv_bucket')

# COMMAND ----------

car_p.rdd.getNumPartitions(),car_c.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
#spark.conf.set("spark.sql.shuffle.partitions", 2)
car_p.join(car_c,'Year').count()

# COMMAND ----------

spark.sql("select * from car_csv_bucket").show(50)

# COMMAND ----------

# MAGIC %md
# MAGIC ###On Emp data  
# MAGIC Even after using bucketBy, spark is still using shuffling. Try using big dataset.

# COMMAND ----------

emp.printSchema()
emp.count()

# COMMAND ----------

emp.rdd.getNumPartitions()

# COMMAND ----------

emp.write.format('parquet').bucketBy(3,'Id').sortBy('Id').mode('overwrite').saveAsTable('emp_parquet_bucket')
emp.write.format('csv').option('header',True).bucketBy(3,'Id').sortBy('Id').mode('overwrite').saveAsTable('emp_csv_bucket')

# COMMAND ----------

emp_p = spark.read.format('parquet').load('dbfs:/user/hive/warehouse/emp_parquet_bucket')
emp_c = spark.read.format('csv').option('header',True).load('dbfs:/user/hive/warehouse/emp_csv_bucket')

# COMMAND ----------

emp_p.rdd.getNumPartitions(),emp_c.rdd.getNumPartitions()

# COMMAND ----------

#spark.conf.set('spark.sql.adaptive.enabled=false ',False)
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)
#spark.conf.set("spark.sql.shuffle.partitions", 200)
emp_p.join(emp_c,'Id').show()

# COMMAND ----------



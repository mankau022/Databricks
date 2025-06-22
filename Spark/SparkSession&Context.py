# Databricks notebook source
print(spark)

# COMMAND ----------

session2 = spark.newSession()
print(session2)

# COMMAND ----------

print(spark.sparkContext)
print(session2.sparkContext)

# COMMAND ----------

df=spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/car_data.csv')

# COMMAND ----------

df.createOrReplaceTempView('df_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended df_table

# COMMAND ----------

spark.catalog.listTables()#returns 2 tables one temporary table and another one created in 'Create table notebook' in sparkSession=spark

# COMMAND ----------

session2.catalog.listTables()#df_table is not returned here, as it was created in sparkSession=spark

# COMMAND ----------

df1=session2.read.format('csv').load('dbfs:/FileStore/tables/car_data.csv')

# COMMAND ----------

df1.createOrReplaceTempView('Session2_table')

# COMMAND ----------

session2.catalog.listTables()

# COMMAND ----------

session3=spark.newSession()

# COMMAND ----------

session3.catalog.listTables()#df_table belongs to sparkSession=spark and 'session2_table' belongs to session2.

# COMMAND ----------

spark.sql("select * from df_table").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table df_table_1
# MAGIC as
# MAGIC select * from df_table;
# MAGIC desc extended df_table_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view vw_table
# MAGIC as
# MAGIC select * from df_table_1;
# MAGIC desc extended vw_table;
# MAGIC --permanent view is allowed by referencing table and not allowed by referencing temp view(createorreplacetempview)

# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions in SQL supported by spark

# COMMAND ----------

#spark.sql("select *,concat(Seller_Type,' ',Transmission) as Seller_Type_Transmission from df_table").display()
#spark.sql("select *,concat_ws(' ',Seller_Type,Transmission) as Seller_Type_Transmission from df_table").display()
#spark.sql("select *,substring(Car_Name,0,len(Car_Name)-2) as Fuel from df_table").display()#removing last 2 characters from Car_Name
#spark.sql("select *,rtrim(ltrim(Fuel_Type)) as Fuel from df_table").display()
#spark.sql("select *,trim(Car_Name) as Car from df_table").display()#removes left and right spaces
#spark.sql("select *,cast(Fuel_Type as string) as Fuel from df_table").display()
#spark.sql("select *,split(Car_Name,'i') as sp,explode(split(Car_Name,'i')) as ex from df_table").display()#explode & split works together as string_split
#spark.sql("select *,case Fuel_Type when 'Petrol' then 'P' when 'Diesel' then 'D' else 'C' end as Fuel from df_table").display()
#spark.sql("select *,charindex('i',Car_Name) as Index from df_table").display()
#spark.sql("select *,left(Car_Name,2) as Car from df_table").display()
#spark.sql("select *,upper(Car_Name) as Car from df_table").display()
#spark.sql("select Car_Name,replace(Car_Name,'i','o') as Car from df_table").display()
#spark.sql("select Car_Name,replace(Car_Name,'alto','car') as Car_R,regexp_replace(Car_Name,'alto','car') as Car_RR from df_table").displa()
#spark.sql("select Car_Name,regexp_extract(Car_Name,'ia',0) as Car from df_table").display()
#spark.sql("select *,reverse(Car_Name) as Car from df_table").display()
#spark.sql("select replace('Manish Kaushik','Manish','Naimish') as Replace").show()
#spark.sql("select regexp_replace('Manish Kaushik','Manish','Naimish') as regexp_replace").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Functions in SQL supported by Spark

# COMMAND ----------

# MAGIC %sql
# MAGIC --select day(current_timestamp()) as Day
# MAGIC --select month(to_date(current_timestamp())) as Month
# MAGIC --select year(to_date(current_timestamp())) as year
# MAGIC --select quarter(to_date(current_timestamp())) as quarter
# MAGIC --select hour(current_timestamp()) as hour
# MAGIC --select minute(current_timestamp()) as min
# MAGIC --select next_day(to_date(current_timestamp()),'Wednesday') as DateonComingWed
# MAGIC --select date_add(to_date(current_timestamp()),5) as DateAdd;--adds day by default
# MAGIC select current_timestamp(),dateadd(hour,5,current_timestamp()) as hour--same as SQL server
# MAGIC --select datediff(to_date(current_timestamp()),'2023-03-10') as DateDiff
# MAGIC --select date(current_timestamp()) as Date
# MAGIC --select date_part('month',current_timestamp()) as Date
# MAGIC /*
# MAGIC select date_format(current_timestamp(),'dd-MM-yyyy') as dd_MM,date_format(current_timestamp(),'MM-dd-yyyy') as MM_dd, date_format(current_timestamp(),'yyyy-MM-dd') as yyyy_MM_dd,date_format(current_timestamp(),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
# MAGIC */
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp(),date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss.SSSS");
# MAGIC --convet to datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC select cast('08/26/2016' as date),to_date('08/26/2016'),to_date('08/26/2016','MM/dd/yyy')
# MAGIC --to_date also takes format as agrument to convert string to date

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --select coalesce(null,null,'manish') as coalesce
# MAGIC --select isnull(null) as isnull
# MAGIC select ifnull(null,'vishal') as ifnull
# MAGIC --select nullif('manish','manish') as nullif
# MAGIC --select cast(current_timestamp() as date) as cast
# MAGIC --select if(1=1,'manish',0 ) as if

# COMMAND ----------

# MAGIC %%sh
# MAGIC python -m site

# COMMAND ----------

pip install pyodbc

# COMMAND ----------

spark.conf.get("spark.executor.memory")

# COMMAND ----------

spark.conf.get("spark.driver.memory")#earlier it used to fail but, after setting this in spark conf while creating cluster 'get' on this property is working fine.
#for 'set' giving error: Cannot modify the value of a Spark config: spark.driver.memory.

# COMMAND ----------

spark.conf.set("spark.driver.memory",'1g')

# COMMAND ----------

spark.conf.get("spark.driver.host")

# COMMAND ----------

spark.conf.get('spark.executor.pyspark.memory')

# COMMAND ----------



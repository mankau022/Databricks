# Databricks notebook source
# MAGIC %sql
# MAGIC create schema Analysis;--Schema and Database are same here
# MAGIC --do not use '/dbfs/FileStore/tables/car_data.csv' complete path as copied. Use as below
# MAGIC --cannot use 'or replace table' in below query as it is supported by V2 sources but it is running fine in command 2 where I have created new table data2 using existing table data1.
# MAGIC create table if not exists Analysis.Data1 using CSV location '/FileStore/tables/car_data.csv';--it will not detect columns
# MAGIC drop table if exists Analysis.Data_Col;
# MAGIC create table if not exists Analysis.Data_Col using CSV options(path '/FileStore/tables/car_data.csv',header 'True', inferSchema "True");--it will detect columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis.Data_Col limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Data2_Col 
# MAGIC as select * from Analysis.Data_Col

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Data2_Col limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists Data2_Col;
# MAGIC drop table if exists Data2;
# MAGIC create table Data2 like Analysis.Data_Col; --Creates Managed table. Like statement do not copies data, just creates table
# MAGIC select * from Data2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,'Manish' as Owner from Analysis.Data_Col--try alias as Owner and it will allow 2 columns with same name
# MAGIC --select * into Data3_Col from Analysis.Data_Col-- into table is not allowed. You can use 'Create table as'

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table data2;
# MAGIC select * from data2;--returenewd no result
# MAGIC drop table data2;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended data2

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from  analysis.data_Col
# MAGIC alter table analysis.data_col add column Car_num int, Car_mileage float

# COMMAND ----------

# MAGIC %sql
# MAGIC --check how we can alter the datatype of a column, giving error when using T-SQl syntax
# MAGIC --alter table analysis.data_col alter column Car_mileage int

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from  analysis.data_Col
# MAGIC describe analysis.data_col

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended analysis;
# MAGIC describe database extended analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended default;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis.data_col

# COMMAND ----------

# MAGIC %sql
# MAGIC --does not support PK and FK constraints
# MAGIC alter table analysis.data_col add constraint ck_Data_col check(length(Car_Name)<20);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended analysis.data_col

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view vw_data_col
# MAGIC as
# MAGIC select * from analysis.data_col where car_name='sx4'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_data_col

# COMMAND ----------

#https://docs.databricks.com/lakehouse/data-objects.html
# use above link to understand difference bt managed and unmanaged(external) tables.
# metastore->catalog->schema/database->tables, view, functions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function func_data_col(car string)
# MAGIC returns table(Car string, Year int)--need to spaecify what column to retun
# MAGIC return select Car_Name,year from analysis.data_col where car_name=car;
# MAGIC
# MAGIC select * from func_data_col('ritz')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended analysis.data_col

# COMMAND ----------

print(spark.catalog.listTables("default"))
print(spark.catalog.listTables("analysis"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table Emp(
# MAGIC   Id int,Name string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *     from Emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Emp values (1,'Manish'),(2,'Vishal');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC update emp set name='Kaushik' where id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------



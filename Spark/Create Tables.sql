-- Databricks notebook source
create table Ext_table using csv options(path '/FileStore/tables/car_data.csv', header 'True', inferSchema "True")
--cannot use replace as table is created using Data_Source. 

-- COMMAND ----------

select * from Ext_table;
--update Ext_table set year=2023  where car_name='ritz'--Failed with error: UPDATE destination only supports Delta sources

-- COMMAND ----------

desc extended Ext_table;--provider=csv; type=External

-- COMMAND ----------

--select * into default.Ext_table_like_parquet_into from Analysis.Ext_table_like_parquet;--Select into is not supported in spark, use CTAS instead.
create or replace table Ext_table_managed--creates a managed delta table
as
select * from Ext_table--provider=csv; type=External

-- COMMAND ----------

desc extended Ext_table_managed--provider=delta; type=Managed

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/ext_table_managed'

-- COMMAND ----------

update Ext_table_managed set year=2023  where car_name='ritz'--updates rows in Managed delta table

-- COMMAND ----------

select * from Ext_table_managed--row is updated

-- COMMAND ----------

delete from  Ext_table_managed where car_name='sx4';

-- COMMAND ----------

delete from  Ext_table where car_name='sx4';--cannot delete/update records in external table

-- COMMAND ----------

alter table Ext_table add column car_mileage varchar(100)--can add column to external table

-- COMMAND ----------

select * from Ext_table limit 3--checking if column is added

-- COMMAND ----------

--create schema Analysis;
create database if not exists analysis Location 'dbfs:/user/hive/warehouse/analysis.db'
create table Analysis.Ext_table_like like Ext_table;--creates a new empty managed(csv) table with same schema in different DB using External(csv) table
select * from Analysis.Ext_table_like--there are no records in table

-- COMMAND ----------

desc extended Analysis.Ext_table_like;--table has provider=csv as provider of parent table Ext_table is also csv.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.listTables()

-- COMMAND ----------

show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

--desc schema Analysis
copy into Analysis.Ext_table_like--COPY INTO target must be a Delta table. Analysis.Ext_table_like's provider=csv
from '/FileStore/tables/'
FILEFORMAT=CSV
FORMAT_OPTIONS(
  'header'='true'
  --'inferSchema'='true'
  )
COPY_OPTIONS(
  'mergeSchema'='true'
  )

-- COMMAND ----------

-- MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/analysis.db/ext_table_like_parquet'

-- COMMAND ----------

--create table Analysis.Ext_table_like_delta like Ext_table using delta;
--`CREATE TABLE LIKE` is not supported for Delta tables. So create table using parquet and then convert it to delta to run COPY INTO
--create or replace table Analysis.Ext_table_like_parquet like Ext_table using parquet;
drop table if exists Analysis.Ext_table_like_parquet; 
create table Analysis.Ext_table_like_parquet like Ext_table using parquet;

-- COMMAND ----------

desc extended Analysis.Ext_table_like_parquet--Managed parquet table

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/analysis.db/ext_table_like_parquet/'

-- COMMAND ----------

insert into Analysis.Ext_table_like_parquet
select * from Ext_table

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/analysis.db/ext_table_like_parquet/'

-- COMMAND ----------

select count(*) from Analysis.Ext_table_like_parquet

-- COMMAND ----------

--convert to delta will not work untill there is a parquet file in folder
--convert to delta `dbfs:/user/hive/warehouse/analysis.db/ext_table_like_parquet`
convert to delta Analysis.Ext_table_like_parquet

-- COMMAND ----------

--desc extended Analysis.Ext_table_like_parquet 
desc history Analysis.Ext_table_like_parquet 

-- COMMAND ----------

select * from Analysis.Ext_table_like_parquet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Delta with COPY INTO  
-- MAGIC Use below link for various file format options  
-- MAGIC https://docs.databricks.com/sql/language-manual/delta-copy-into.html#csv-options

-- COMMAND ----------

--COPY INTO target must be a Delta table. Analysis.Ext_table_like's provider=csv
copy into Analysis.Ext_table_like_parquet
from '/FileStore/tables/car_data.csv'
FILEFORMAT=CSV
FORMAT_OPTIONS(
  'header'='true',
  'inferSchema'='true'
  )
COPY_OPTIONS(
  'force'='false',--if true, idempotency is disabled and files will be loaded regardless they have been loaded earlier.
  'mergeSchema'='true'
  )

-- COMMAND ----------

select count(*) from Analysis.Ext_table_like_parquet--number of rows are doubled

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #removing all tables from Analysis DB
-- MAGIC #spark.catalog.listTables()#for current DB
-- MAGIC table_list = spark.catalog.listTables('Analysis')
-- MAGIC for tables in table_list:
-- MAGIC   #query = 'drop table if exists Analysis.'+tables.name
-- MAGIC   #print(query)
-- MAGIC   spark.sql('drop table if exists Analysis.'+tables.name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.listTables('Analysis')#all tables deleted

-- COMMAND ----------

create or replace table table_created
(id int,
name string);
desc extended table_created--provider=delta supports replace statement.

-- COMMAND ----------

drop table if exists table_created_csv;
create table table_created_csv
(id int,
name string) using csv--remove 'using csv' and you can use 'create or replace'(for delta)
location 'dbfs:/user/hive/warehouse/table_created_csv';--remove 'location' and table will be 'Managed'
desc extended table_created_csv

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database if exists analysis

-- COMMAND ----------

show databases

-- COMMAND ----------

create database if not exists analysis Location 'dbfs:/user/hive/warehouse/analysis.db'--run it to create database after cluster termination

-- COMMAND ----------

CREATE TABLE student (id INT, name STRING, age INT) STORED AS ORC;--type=Managed, provider=hive

-- COMMAND ----------

desc extended student

-- COMMAND ----------



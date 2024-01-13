# Databricks notebook source
# the schmea that we we will use to hold data, let's it is testdb.
# Here this is basic example, where we will have  3 tables named Orders, Delivery and Billing
# Suppose Orders table has one to one relation with both Delivery and Billing table.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cleaning up existing things
# MAGIC

# COMMAND ----------

# DBTITLE 1,Dropping the database if exists
# MAGIC %sql
# MAGIC drop schema if exists testdb cascade;

# COMMAND ----------

# DBTITLE 1,deleting the default databricks folder for the database

dbFolderExists = None
dbBasePath = "dbfs:/user/hive/warehouse/testdb.db"
try:
  dbutils.fs.ls( dbBasePath )
  print("folder exists")
  dbFolderExists = True
except Exception as e:
  if "java.io.FileNotFoundException" in e.args[0]:
    print("folder does not exists")
    dbFolderExists = False
  else:
    raise e


if dbFolderExists:
  print("Removing the database folder")
  dbutils.fs.rm( dbBasePath, True )

# COMMAND ----------

# MAGIC %sql
# MAGIC create database testdb;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table testdb.Orders( order_no string, order_qty int, order_creation_date date  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.Delivery( order_no string, delivery_address string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.Billing( order_no string, billing_address string)
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into testdb.Orders( order_no, order_qty, order_creation_date )
# MAGIC values('Ord1', 7, '2023-08-02'), ('Ord2', 55, '2023-08-07' ), ('Ord3', 12, '2023-07-25' );
# MAGIC
# MAGIC insert into testdb.Delivery( order_no, delivery_address  ) 
# MAGIC values('Ord1', 'Museum street, Hyderabad'), ('Ord2', 'Marathali Lane 6, Bangalore');
# MAGIC
# MAGIC insert into testdb.Billing( order_no, billing_address  )
# MAGIC values ('Ord1', 'Hi tech city, Hyderabad'), ('Ord3', 'OMP square, Cuttack');
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("end")
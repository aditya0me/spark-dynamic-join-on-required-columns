# Databricks notebook source
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
# MAGIC create or replace table testdb.Orders( order_no string, order_qty int, order_creation_date date, item_id string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.Items( item_id string, item_name string, pricePerUnit int  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.Delivery( delivery_no string, delivery_date date, delivery_address string, order_no string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.Billing( billing_no string, billing_amount int, billing_address string, order_no string)
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.CancelledOrders( order_no string, reason_for_cancellation string)
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into testdb.Orders( order_no, order_qty, order_creation_date, item_id )
# MAGIC values('Ord1', 7, '2023-08-02', 'ITM-4' ), ('Ord2', 55, '2023-08-07', 'ITM-1' ), ('Ord3', 2, '2023-07-25', 'ITM-2' ), ('Ord4', 1, '2023-08-15', 'ITM-3' ), ('Ord5', 24, '2023-08-25', 'ITM-1' ), ('Ord6', 2, '2023-09-03', 'ITM-6' );
# MAGIC
# MAGIC insert into testdb.Items( item_id, item_name, pricePerUnit)
# MAGIC values('ITM-1', 'Good-day biscuit', 12), ('ITM-2', 'Samsung television', 14999), ('ITM-3', 'adidas football',2059), ('ITM-4', 'The psychology of money', 499 ), ('ITM-6', 'Jumbo t-shirt', 959 );
# MAGIC
# MAGIC insert into testdb.Delivery( delivery_no, delivery_date, delivery_address, order_no ) 
# MAGIC values('DelvNo75', '2023-08-11', 'Lane 6, Bangalore', 'Ord3'  ), ('DelvNo40', '2023-09-07', 'HRC Colony, Bhubaneswar', 'Ord5' ), ('DelvNo12', '2023-08-20', 'Museum street, Hyderabad', 'Ord1' );
# MAGIC
# MAGIC insert into testdb.Billing( billing_no, billing_amount, billing_address, order_no )
# MAGIC values('BillNo75', 270, 'HRC Colony, Bhubaneswar', 'Ord5'  ), ('BillNo40', 3493, 'Hi tech city, Hyderabad', 'Ord1' ), ('BillNo12', 2000, 'OMP square, Cuttack', 'Ord4' );
# MAGIC
# MAGIC insert into testdb.CancelledOrders( order_no, reason_for_cancellation  )
# MAGIC values('Ord6', 'found a better product');

# COMMAND ----------

dbutils.notebook.exit()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.VBUK;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.VBAP;
# Databricks notebook source
# MAGIC %md
# MAGIC ####Cleaning up existing things
# MAGIC

# COMMAND ----------

# DBTITLE 1,Dropping the database if exists
# MAGIC %sql
# MAGIC -- drop database if exists testdb;
# MAGIC
# MAGIC drop schema if exists testdb cascade;

# COMMAND ----------

# DBTITLE 1,deleting the folder for the database

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
  dbutils.fs.rm( dbBasePath, True )

# COMMAND ----------

# MAGIC %sql
# MAGIC create database testdb;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table testdb.VBAP( VBELN string, POSNR string, ERDAT string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.VBAK( VBELN string, ERDAT string, BSTNK string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.VBUK( VBELN string, CMGST string  )
# MAGIC using delta;
# MAGIC
# MAGIC create or replace table testdb.MUST_JOIN_TEST_TABLE_1( VBELN string  )
# MAGIC using delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into testdb.VBAP( VBELN, POSNR, ERDAT  ) 
# MAGIC values('O1', 'L1', '20230802' ), ('O1', 'L2', '20230803' ), ('O2', 'L1', '20230810' ),('O3', 'L1', '20230805' );
# MAGIC
# MAGIC insert into testdb.VBAK( VBELN, ERDAT, BSTNK  ) 
# MAGIC values('O1', '20230725', 'PO1_bjaf' ), ('O2', '20230728', 'PO2_ajnsdi' ), ('O3', '20230721', 'PO3_ndne' );
# MAGIC
# MAGIC insert into testdb.VBUK( VBELN, CMGST  ) 
# MAGIC values('O1', 'STS_1' ), ('O1', 'STS_1' ), ('O2', 'STS_7' );
# MAGIC
# MAGIC insert into testdb.MUST_JOIN_TEST_TABLE_1( VBELN  ) 
# MAGIC values('O3');

# COMMAND ----------

dbutils.notebook.exit()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.VBUK;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testdb.VBAP;
# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../DynamicJoin

# COMMAND ----------

def getTablesInfoDictSample2():
  tablesInfoDict = {
    "vbap" : {
      "dataframeVar" :  spark.read.table( "testdb.VBAP" )
      ,"alias" : "vbap"
      ,"isStartingTable": True
      ,"requiredT":True
      ,"joinConditionExpresion": "STARTING_TABLE"
      ,"joinType": "STARTING_TABLE"
      ,"joinDependentOnTables":["STARTING_TABLE"]
      ,"tableJoinSeq" : 1  #UseIfNeeded
      }
    ,"must_join_test_table_1" : {
      "dataframeVar" : spark.read.table( "testdb.MUST_JOIN_TEST_TABLE_1" )
      ,"alias" : "must_join_test_table_1"
      ,"isStartingTable": False
      ,"requiredT": False #Will update it if needed
      ,"joinConditionExpresion" : ( col("vbak.VBELN") == col("must_join_test_table_1.VBELN")  )
      ,"joinType": "inner"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : 4  #UseIfNeeded
      }
    ,"vbak" : {
      "dataframeVar" : spark.read.table( "testdb.VBAK" )
      ,"alias" : "vbak"
      ,"isStartingTable": False
      ,"requiredT": True
      ,"joinConditionExpresion" : ( col("vbap.VBELN") == col("vbak.VBELN")  )
      ,"joinType": "inner"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : 2  #UseIfNeeded
      }
    ,"vbuk" : {
      "dataframeVar" : spark.read.table( "testdb.VBUK" )
      ,"alias" : "vbuk"
      ,"isStartingTable": False
      ,"requiredT": False #Will update it if needed
      ,"joinConditionExpresion" : ( col("vbap.VBELN") == col("vbuk.VBELN")  )
      ,"joinType": "left"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : 3  #UseIfNeeded
      }
    ,"must_join_test_table_1" : {
      "dataframeVar" : spark.read.table( "testdb.MUST_JOIN_TEST_TABLE_1" )
      ,"alias" : "must_join_test_table_1"
      ,"isStartingTable": False
      ,"requiredT": False #Will update it if needed
      ,"joinConditionExpresion" : ( col("vbak.VBELN") == col("must_join_test_table_1.VBELN")  )
      ,"joinType": "left_anti"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : 5  #UseIfNeeded
      }
    }
  return tablesInfoDict

def getColumnsInfoDictSample2():
  columnsInfoDict = {
    "SLS_ORD_NO" : {
      "expr" : ( col("vbap.VBELN") )
      ,"dependentOnTables" : [ "vbap" ]
      , "requiredC" : True
      }
    , "SLS_ORD_LN_NO" : {
      "expr" : ( col("vbap.POSNR") )
      ,"dependentOnTables" : [ "vbap" ] 
      , "requiredC" : True
    }
    ,"CRT_DT_ITM" : {
      "expr" : ( col("vbap.ERDAT") )
      ,"dependentOnTables" : [ "vbap" ]
      , "requiredC" : False
    }
    ,"CRT_DT_HDR" : {
      "expr" : ( col("vbak.ERDAT") )
      ,"dependentOnTables" : [ "vbak" ]
      , "requiredC" : False
    }
    ,"CR_CHK_TOT_STS_CD" : {
      "expr" : ( col("vbuk.CMGST") )
      ,"dependentOnTables" : [ "vbuk" ] 
      , "requiredC" : False
    }
  }
  return columnsInfoDict

# COMMAND ----------

tablesInfoDict = 
columnsInfoDict = 
heheDf = newApproach(tablesInfoDict, columnsInfoDict, allColumns =  True, joinTableKeyIrrespectiveOfColumnSelectedList = ["must_join_test_table_1"] )
heheDf.display()

# COMMAND ----------

selectedColDf = newApproach( columnListToSelect =  ["CRT_DT_ITM", "CRT_DT_HDR"] )
selectedColDf.display()
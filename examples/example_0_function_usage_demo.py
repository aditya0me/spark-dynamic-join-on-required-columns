# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../DynamicJoin

# COMMAND ----------

def getTablesInfoDictExample0():
  tablesInfoDict = {
    "ordersTabKey" : {
      "dataframeVar" :  spark.read.table( "testdb.Orders" )
      ,"alias" : "orders_Alias"
      ,"isStartingTable": True
      ,"requiredT":True
      ,"joinConditionExpresion": None
      ,"joinType": None
      ,"joinDependentOnTables":[]
      ,"tableJoinSeq" : 0
      }
    ,"deliveryTabKey" : {
      "dataframeVar" : spark.read.table( "testdb.Delivery" )
      ,"alias" : "delivery_Alias"
      ,"isStartingTable": False
      ,"requiredT": False
      ,"joinConditionExpresion" : ( col("orders_Alias.order_no") == col("delivery_Alias.order_no")  )
      ,"joinType": "left"
      ,"joinDependentOnTables":["ordersTabKey"]
      ,"tableJoinSeq" : 2
      }
    ,"billingTabKey" : {
      "dataframeVar" : spark.read.table( "testdb.Billing" )
      ,"alias" : "billing_Alias"
      ,"isStartingTable": False
      ,"requiredT": False
      ,"joinConditionExpresion" : ( col("orders_Alias.order_no") == col("billing_Alias.order_no")  )
      ,"joinType": "left"
      ,"joinDependentOnTables":["ordersTabKey"]
      ,"tableJoinSeq" : 3
      }
    }
  
  return tablesInfoDict


def getColumnsInfoDictExample0():
  columnsInfoDict = {
    "OrderNum" : {
      "expr" : ( col("orders_Alias.order_no") )
      ,"dependentOnTables" : [ "ordersTabKey" ]
      , "requiredC" : True #This columns must be there as these are the staring columns of the object
      }
    ,"QtyOrdered" : {
      "expr" : ( col("orders_Alias.order_qty") )
      ,"dependentOnTables" : [ "ordersTabKey" ]
      , "requiredC" : False
    }
    ,"OrderCreationDate" : {
      "expr" : ( col("orders_Alias.order_creation_date") )
      ,"dependentOnTables" : [ "ordersTabKey" ]
      , "requiredC" : False
    }
    ,"BillingAddress" : {
      "expr" : ( col("billing_Alias.billing_address") )
      ,"dependentOnTables" : [ "billingTabKey" ] 
      , "requiredC" : False
    }
    ,"Delivery_Address" : {
      "expr" : ( col("delivery_Alias.delivery_address") )
      ,"dependentOnTables" : [ "deliveryTabKey" ] 
      , "requiredC" : False
    }
    
  }

  return columnsInfoDict



# COMMAND ----------

tablesInfoDict = getTablesInfoDictExample0()
columnsInfoDict = getColumnsInfoDictExample0()
columnsToSelect = [ "OrderNum", "QtyOrdered", "BillingAddress" ]


ordersReqColumnsDf = joinTablesAndSelectExprDynamically(tablesInfoDict, columnsInfoDict, columnListToSelect = columnsToSelect )
ordersReqColumnsDf.display()
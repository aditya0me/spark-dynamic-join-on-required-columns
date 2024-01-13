# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../DynamicJoin

# COMMAND ----------

def getTablesInfoDictSample1():
  tablesInfoDict_SampleData1 = {
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
    ,"itemsTabKey" : {
      "dataframeVar" : spark.read.table( "testdb.Items" )
      ,"alias" : "items_Alias"
      ,"isStartingTable": False
      ,"requiredT": False #Will update it later when needed
      ,"joinConditionExpresion" : ( col("orders_Alias.item_id") == col("items_Alias.item_id")  )
      ,"joinType": "inner"
      ,"joinDependentOnTables":["ordersTabKey"]
      ,"tableJoinSeq" : 1 
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
    ,"cancelledOrdersTabKey" : {
      "dataframeVar" : spark.read.table( "testdb.CancelledOrders" )
      ,"alias" : "cancelled_orders_Alias"
      ,"isStartingTable": False
      ,"requiredT": False
      ,"joinConditionExpresion" : ( col("orders_Alias.order_no") == col("cancelled_orders_Alias.order_no")  )
      ,"joinType": "left_anti"
      ,"joinDependentOnTables":["ordersTabKey"]
      ,"tableJoinSeq" : 4
      }
    }
  
  return tablesInfoDict_SampleData1


def getColumnsInfoDictSample1():
  columnsInfoDict_SampleData1 = {
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
    ,"OrderedItemId" : {
      "expr" : ( col("orders_Alias.item_id") )
      ,"dependentOnTables" : [ "ordersTabKey" ] 
      , "requiredC" : False
    }
    ,"OrderedItemName" : {
      "expr" : ( col("items_Alias.item_name") )
      ,"dependentOnTables" : [ "itemsTabKey" ] 
      , "requiredC" : False
    }  
    ,"ItemPricePerUnit" : {
      "expr" : ( col("items_Alias.pricePerUnit") )
      ,"dependentOnTables" : [ "itemsTabKey" ] 
      , "requiredC" : False 
    }

    ,"BillingAmount" : {
      "expr" : ( col("billing_Alias.billing_amount") )
      ,"dependentOnTables" : [ "billingTabKey" ] 
      , "requiredC" : False
    }
    ,"BillingAddress" : {
      "expr" : ( col("billing_Alias.billing_address") )
      ,"dependentOnTables" : [ "billingTabKey" ] 
      , "requiredC" : False
    }
    ,"DeliveryDate" : {
      "expr" : ( col("delivery_Alias.delivery_date") )
      ,"dependentOnTables" : [ "deliveryTabKey" ] 
      , "requiredC" : False
    }
    ,"Delivery_Address" : {
      "expr" : ( col("delivery_Alias.delivery_address") )
      ,"dependentOnTables" : [ "deliveryTabKey" ] 
      , "requiredC" : False
    }
    
  }

  return columnsInfoDict_SampleData1



# COMMAND ----------

# DBTITLE 1,Case - 1
#Case - 1 
tablesInfoDictCase1 = getTablesInfoDictSample1()
columnsInfoDictCase1 = getColumnsInfoDictSample1()


case1ResDf = joinTablesAndSelectExprDynamically(tablesInfoDictCase1, columnsInfoDictCase1, allColumns =  True, joinTableKeyIrrespectiveOfColumnSelectedList = [] )
case1ResDf.display()

# COMMAND ----------

# DBTITLE 1,Case - 2
#Case - 2
tablesInfoDictCase2 = getTablesInfoDictSample1()
columnsInfoDictCase2 = getColumnsInfoDictSample1()


case2ResDf = joinTablesAndSelectExprDynamically(tablesInfoDictCase2, columnsInfoDictCase2, allColumns =  True, joinTableKeyIrrespectiveOfColumnSelectedList = ["cancelledOrdersTabKey"] )
case2ResDf.display()

# COMMAND ----------

# DBTITLE 1,Case - 3
#Case - 3
tablesInfoDictCase3 = getTablesInfoDictSample1()
columnsInfoDictCase3 = getColumnsInfoDictSample1()
columnsToSelectIn3rdCase = [ "OrderNum", "QtyOrdered", "ItemPricePerUnit", "BillingAmount" ]

case3ResDf = joinTablesAndSelectExprDynamically(tablesInfoDictCase3, columnsInfoDictCase3, columnListToSelect=columnsToSelectIn3rdCase )
case3ResDf.display()

# COMMAND ----------

# DBTITLE 1,Case - 4
#Case - 4
tablesInfoDictCase4 = getTablesInfoDictSample1()
columnsInfoDictCase4 = getColumnsInfoDictSample1()
columnsToSelectIn4thTestCase = [ "OrderNum", "OrderCreationDate", "DeliveryDate", "Delivery_Address" ]

case4ResDf = joinTablesAndSelectExprDynamically(tablesInfoDictCase4, columnsInfoDictCase4, columnListToSelect=columnsToSelectIn4thTestCase )
case4ResDf.display()
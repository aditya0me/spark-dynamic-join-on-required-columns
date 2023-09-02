# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../DynamicJoin

# COMMAND ----------

tablesInfoDict_SampleData2 = {
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


columnsInfoDict_SampleData2 = {
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



# COMMAND ----------

hehe1Df = newApproach(tablesInfoDict_SampleData2, columnsInfoDict_SampleData2, allColumns =  True, joinTableKeyIrrespectiveOfColumnSelectedList = [] )
hehe1Df.display()


hehe2Df = newApproach(tablesInfoDict_SampleData2, columnsInfoDict_SampleData2, allColumns =  True, joinTableKeyIrrespectiveOfColumnSelectedList = ["cancelledOrdersTabKey"] )
hehe2Df.display()



# COMMAND ----------

columnsToSelectIn3rdTestCase = [ "OrderNum", "QtyOrdered", "ItemPricePerUnit", "BillingAmount" ]

hehe3Df = newApproach(tablesInfoDict_SampleData2, columnsInfoDict_SampleData2, columnListToSelect=columnsToSelectIn3rdTestCase )
hehe3Df.display()


# COMMAND ----------

columnsToSelectIn4thTestCase = [ "OrderNum", "OrderCreationDate", "DeliveryDate", "Delivery_Address" ]

hehe4Df = newApproach(tablesInfoDict_SampleData2, columnsInfoDict_SampleData2, columnListToSelect=columnsToSelectIn4thTestCase )
hehe4Df.display()

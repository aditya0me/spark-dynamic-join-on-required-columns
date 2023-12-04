# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Sample data One
def getOrderInfo():
  ordersDf = spark.read.table("testdb.Orders")
  itemsDf = spark.read.table("testdb.Items")
  deliveryDf = spark.read.table("testdb.Delivery")
  billingDf = spark.read.table("testdb.Billing")

  orderInfoDf = (
    ordersDf
      .join(itemsDf
            , ordersDf.item_id == itemsDf.item_id
            , "inner"
            )
      .join(deliveryDf
            , ordersDf.order_no == deliveryDf.order_no
            , "left"
            )
      .join(billingDf
            , billingDf.order_no == billingDf.order_no
            ,"left" 
            )
      .select(
        ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_qty.alias("QtyOrdered")
        , ordersDf.order_creation_date.alias("OrderCreationDate")
        , ordersDf.item_id.alias("OrderedItemId")
        , ordersDf.item_name.alias("OrderedItemName")
        , ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_no.alias("OrderNum")

      )
  )




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

def currentApproach():
  vbapObject = spark.read.table("testdb.VBAP")
  vbakObject = spark.read.table("testdb.VBAK")
  vbukObject = spark.read.table("testdb.VBUK")


  salesOrderObject = (
    vbapObject
      .join( vbakObject
            , vbapObject.VBELN == vbakObject.VBELN
            , "inner"
      )
      .join( vbukObject
            , vbapObject.VBELN == vbukObject.VBELN
            , "left_outer"
      )
      .select(
        vbapObject.VBELN.alias("SLS_ORD_NO")
        , vbapObject.POSNR.alias("SLS_ORD_LN_NO")
        , vbapObject.ERDAT.alias("CRT_DT_ITM")
        , vbakObject.ERDAT.alias("CRT_DT_HDR")

        , vbukObject.CMGST.alias("CR_CHK_TOT_STS_CD")
        
      )
  )

  return salesOrderObject

# COMMAND ----------

resDf = currentApproach()
resDf.display()

# COMMAND ----------

vbapObject = spark.read.table("testdb.VBAP")
vbakObject = spark.read.table("testdb.VBAK")

tdf = (
  vbapObject.alias("vbap")
      .join( vbakObject.alias("vbak")
            , vbapObject.VBELN == vbakObject.VBELN
            , "inner"
      )
)

vdf = (
  tdf.select(
    col("vbap.VBELN")
    ,col("vbap.ERDAT")

  )
)

# COMMAND ----------

vdf.display()
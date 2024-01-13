# Databricks notebook source
def getOrdersInfoDataFrame():
  ordersDf = spark.read.table("testdb.Orders")
  deliveryDf = spark.read.table("testdb.Delivery")
  billingDf = spark.read.table("testdb.Billing")

  orderInfoDf = (
    ordersDf
      .join(deliveryDf
            , ordersDf.order_no == deliveryDf.order_no
            , "left"
            )
      .join(billingDf
            , ordersDf.order_no == billingDf.order_no
            ,"left" 
            )
      .select(
        ordersDf.order_no.alias("OrderNum")
        , ordersDf.order_qty.alias("QtyOrdered")
        , ordersDf.order_creation_date.alias("OrderCreationDate")
        , billingDf.billing_address.alias("BillingAddress")
        , deliveryDf.delivery_address.alias("Delivery_Address")
      )
  )
  return orderInfoDf
  

# COMMAND ----------

ordersDf = getOrdersInfoDataFrame()
ordersReqColumnsDf = ordersDf.select("OrderNum", "QtyOrdered", "BillingAddress")
ordersReqColumnsDf.display()
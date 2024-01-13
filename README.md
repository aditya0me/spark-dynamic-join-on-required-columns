## Note
Right now the python files this repository is having are actually datbricks notebooks. I am using the communtiy edition datbricks which is free to use, experiment and learning.<br>Although the python files are datbarick notebooks, the function can definitely be used in other spark environment. Will change that into normal python format in future changes.

## Problem statement this solves
Let's discuss how this will help with an example. The example I am going to discuss here is present in the examples folder with name as example_0. And you can also go through the files in the order [example_0_data_seeder](/examples/example_0_data_seeder.py), [example_0_direct_approach](/examples/example_0_direct_approach.py), [example_0_function_usage_demo](/examples/example_0_function_usage_demo.py).<br>That is being discussed here also. 
Let's say we have 3 tables with schema as - 
- Orders(order_no : string, order_qty : int, order_creation_date : date)
- Delivery(order_no : string, delivery_address : string)
- Billing(order_no : string, billing_address : string) 

And Orders table has one to one relation with both Delivery and Billing table. Suppose you are having a factory method which gives the orders information dataframe by joining the tables and giving alias to the appropriate columns. And you use this dataframe at multiple places while selecting different columns as needed.

```
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
```

Let's say at a place we need the orders info dataframe but we only need billing information. So what we can do is call the above function and select the required billing columns. That code will be as below.
```
ordersDf = getOrdersInfoDataFrame()
ordersReqColumnsDf = ordersDf.select("OrderNum", "QtyOrdered", "BillingAddress")
ordersReqColumnsDf.show()
```

If you see the physical plan for the above spark action, here even though we are not selecting any Delivery table colummn but it (Delivery) table will be read and joined. All though spark is intelligent enough to only read *order_no* column from Delivery table and won't read the delivery_address column as it is not needed anywhere, but at least the join operation has to be performed as it is there in the expression.

![Joining all the tables and selecting column spark plan in version 3.4.1 on sample dataset](/assets/images/example_0_direct_join_spark_plan_3.4.1.png)<br>*Joining all the tables and selecting column spark plan in version 3.4.1 on sample small dataset*

## New Approach
Here we keep the tables join information and columns transformation information in a format (we can call it a data structure) to facilitate join. Here we will maintain two dictionaries i) one holding required information about tables that are part of join ii)second holding the columns information and their transformations. For the above example the code for the above approach is as below.
```
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

columnsToSelect = [ "OrderNum", "QtyOrdered", "BillingAddress" ]

ordersReqColumnsDf = joinTablesAndSelectExprDynamically(tablesInfoDict, columnsInfoDict, columnListToSelect = columnsToSelect )
ordersReqColumnsDf.show()
```
If you see the physical plan for the spark action in new approach, you will see the Delivery table is not being joined as only Billing table column is used.

![New approach spark plan in version 3.4.1 on sample small dataset](/assets/images/example_0_new_approach_function_join_spark_plan_3.4.1.png)<br>*New approach spark plan in version 3.4.1 on sample small dataset*

## Ending note - Scenarios supporting this approach
Here, there is only 3 tables are joined and a small set of columns are only there. But think there are many tables are joined and the columns are with some transformation logic, then the discussed new approach will make more sense when we don't want to repeat the logic and keep at a place. Also in terms of cost, it will be much better as we are removing the unnecessary join of tables.
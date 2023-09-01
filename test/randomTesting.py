# Databricks notebook source
vbapObject = spark.read.table("testdb.VBAP")
vbakObject = spark.read.table("testdb.VBAK")
vbukObject = spark.read.table("testdb.VBUK")

must_join_test_table_1_Object = spark.read.table( "testdb.MUST_JOIN_TEST_TABLE_1" )
salesOrderObject = (
  vbapObject
  .join( vbakObject
          , vbapObject.VBELN == vbakObject.VBELN
          , "inner"
    )
    .join( must_join_test_table_1_Object
          , ( vbakObject.VBELN == must_join_test_table_1_Object.VBELN )
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
      # , vbakObject.ERDAT.alias("CRT_DT_HDR")

      , vbukObject.CMGST.alias("CR_CHK_TOT_STS_CD")
      
    )
)

salesOrderObject.display()

# COMMAND ----------


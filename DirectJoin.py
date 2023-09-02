# Databricks notebook source
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

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
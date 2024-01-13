# Databricks notebook source
# MAGIC %run ../util/ValidatingTablesInfoDict

# COMMAND ----------

# MAGIC %run ../util/ValidatingColumnsInfoDict

# COMMAND ----------

#copy this table info dict  and column info dict from any of the examples out there 
def getTablesInfoDictSample():
  return


def getColumnsInfoDictSample():
  return 

# COMMAND ----------

tabInfo = getTablesInfoDictSample()
ValidatingTablesInfoDict.runValidation( tabInfo )

# COMMAND ----------

tabInfo = getTablesInfoDictSample()
colInfo = getColumnsInfoDictSample()
ValidatingColumnsInfoDict.checkIfAllDependentOnTableKeysExistInTablesInfoDict( tabInfo, colInfo )
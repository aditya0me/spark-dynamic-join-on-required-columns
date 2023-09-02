# Databricks notebook source
def printTableInfoDictStatus( tablesInfoDictInp:dict ):
  for key in tablesInfoDictInp:
    print( f"""{key}\n--------------\nrequiredT={tablesInfoDictInp[key]["requiredT"]}""" )

# COMMAND ----------

def updateTableRequiredStatusRecursively(tablesInfoDict:dict, currentTableNameKey:str  ):
  #Todo: Check if there is a chance of falling in inifinte recursion like cycles in depth first search. We should develop an utility function to check if any cycle exists. We should identify before hands for good customer experience. If we will be carefull enough it will be fine

  if tablesInfoDict[currentTableNameKey]["requiredT"]:  #Todo-class would have accessed by functions 
    return #base case or if one table already made true that means no need to check further for dependent tables, as it would have already been taken care

  for joinDependentTable in tablesInfoDict[currentTableNameKey]["joinDependentOnTables"]:
    updateTableRequiredStatusRecursively(tablesInfoDict, joinDependentTable  )
  
  tablesInfoDict[currentTableNameKey]["requiredT"] = True
  return
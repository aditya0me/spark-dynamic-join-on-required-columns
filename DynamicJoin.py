# Databricks notebook source
# MAGIC %run ./util/UtilityFunctions

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
import copy

# COMMAND ----------


def joinTablesAndSelectExprDynamically( tablesInfoDict:dict, columnsInfoDict:dict, columnListToSelect: list[str]=None, allColumns:bool=False, 
joinTableKeyIrrespectiveOfColumnSelectedList:list[str] = []  ):
  
  #Step 1 - Can call the validation functions here, so that they if the tablesInfo and columnsInfo is not given properly we will fail them with a good error message so that it can be caught easily


  # Step 2- Now to generate the select expression of required columns, for this i) we wll update their required status ii)and also in the way update the required status of tables iii) generate the select expression in a list
  # Step 2.i----
  if allColumns:
    #Update all columns required status to True
    for colNameKey in columnsInfoDict:
      columnsInfoDict[ colNameKey ]["requiredC"] = True  #Todo:For these reasons thinking to convert into classes so that we can create a member function
  else:
    for colNameKey in columnListToSelect:
      if colNameKey not in columnsInfoDict:
        raise Exception(f"""input column {colNameKey} does not exist in columns info dictionary""")
      else:
        columnsInfoDict[ colNameKey ]["requiredC"] = True
  
  # Step 2.ii and 2.iii----
  requiredColumnsSelectExprList = [  ]
  for colNameKey in columnsInfoDict:
    if not columnsInfoDict[colNameKey]["requiredC"]:
      continue
    for curColReqTable in columnsInfoDict[colNameKey]["dependentOnTables"]:
      updateTableRequiredStatusRecursively( tablesInfoDict , curColReqTable )
    #Will add the expression for thr column to the list
    requiredColumnsSelectExprList.append( columnsInfoDict[colNameKey]["expr"].alias( colNameKey )  ) 
  

  #Step 3-There will be scenarios where we must need to join a table irrespective of any columns selected or not. For one example there is a table and we are doing left anti join on it. So in this case we won't be selecting any columns from it, we can't set the required flag for the table from columnsInfoDict. So we are having an argument in the function  joinTableKeyIrrespectiveOfColumnSelectedList. Will iterate on this list and set the required status flag for the table.
  for tabNameItr in joinTableKeyIrrespectiveOfColumnSelectedList:
    tablesInfoDict[tabNameItr]["requiredT"] = True 

  print( requiredColumnsSelectExprList  )
  printTableInfoDictStatus( tablesInfoDict )
 
  # Step 4- To join the only required tables and keep it in a intermediate expression
  # First we will sort the tables  key in ascending sequence no as in dictionary they can appear in any order (although in python the sequence in dict is reserved)
  tableNameKeyInAcsendingSeqOrder = [ (tabNameKey, tablesInfoDict[tabNameKey]["tableJoinSeq"]) for tabNameKey in tablesInfoDict  ]  #Here it is not in ascending order but in next step we are sorting
  tableNameKeyInAcsendingSeqOrder.sort( key = lambda x: x[1] )

  startingTableKey = None   #Either we can write it manually otherwise we can generate from info Dict... Info dict is seeming better to extract from 
  for tabNameKey in tablesInfoDict:
    if tablesInfoDict[tabNameKey]["isStartingTable"]:
      startingTableKey = tabNameKey
      break
  
  requiredTableJoinExpression = ( tablesInfoDict[startingTableKey]["dataframeVar"].alias( tablesInfoDict[startingTableKey]["alias"]  ) 
                                 )  #will start from the starting table
 
  #Will iterate for other required tables join
  for tabNameKey, _ in tableNameKeyInAcsendingSeqOrder:
    if (startingTableKey == tabNameKey) or ( not tablesInfoDict[tabNameKey]["requiredT"] ):
      continue #As starting table is already in the join expression and joins for not required columns is not needed
    
    currentTableInfoDict = tablesInfoDict[ tabNameKey ]
    currentTableDataFrameAlongWithAlias  = tablesInfoDict[tabNameKey]["dataframeVar"].alias( tablesInfoDict[tabNameKey]["alias"]  ) 
    requiredTableJoinExpression = requiredTableJoinExpression.join(other= currentTableDataFrameAlongWithAlias, on= currentTableInfoDict["joinConditionExpresion"] , how=currentTableInfoDict["joinType"])

  
  resultDf = requiredTableJoinExpression.select( *requiredColumnsSelectExprList )
  return resultDf

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

# COMMAND ----------

# MAGIC %md
# MAGIC ####Proposed Approach

# COMMAND ----------

# DBTITLE 1,Utility functions
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

# COMMAND ----------


def newApproach( allColumns:bool=False, columnListToSelect: list[str]=None  ):
  # There would be a or join of couple of tables joined which will be starting table for our business object
  
  # vbapObject = spark.read.table("testdb.VBAP")
  # startingTablesJoinedDf = (
  #   vbapObject.alias("vbap")
  # )
  tablesInfoDict = {
    "vbap" : {
      "dataframeVar" :  spark.read.table( "testdb.VBAP" )
      ,"alias" : "vbap"
      ,"isStartingTable": True
      ,"requiredT":True
      ,"joinConditionExpresion": "STARTING_TABLE"
      ,"joinType": "STARTING_TABLE"
      ,"joinDependentOnTables":["STARTING_TABLE"]
      ,"tableJoinSeq" : None  #UseIfNeeded
      }
    ,"vbak" : {
      "dataframeVar" : spark.read.table( "testdb.VBAK" )
      ,"alias" : "vbak"
      ,"isStartingTable": False
      ,"requiredT": True
      ,"joinConditionExpresion" : ( col("vbap.VBELN") == col("vbak.VBELN")  )
      ,"joinType": "inner"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : None  #UseIfNeeded
      }
    ,"vbuk" : {
      "dataframeVar" : spark.read.table( "testdb.VBUK" )
      ,"alias" : "vbuk"
      ,"isStartingTable": False
      ,"requiredT": False #Will update it if needed
      ,"joinConditionExpresion" : ( col("vbap.VBELN") == col("vbuk.VBELN")  )
      ,"joinType": "left"
      ,"joinDependentOnTables":["vbap"]
      ,"tableJoinSeq" : None  #UseIfNeeded
      }
    }


  columnsInfoDict = {
    "SLS_ORD_NO" : {
      "expr" : ( col("vbap.VBELN") )
      ,"dependentOnTables" : [ "vbap" ]
      , "requiredC" : True
      }
    , "SLS_ORD_LN_NO" : {
      "expr" : ( col("vbap.POSNR") )
      ,"dependentOnTables" : [ "vbap" ] 
      , "requiredC" : True
    }
    ,"CRT_DT_ITM" : {
      "expr" : ( col("vbap.ERDAT") )
      ,"dependentOnTables" : [ "vbap" ]
      , "requiredC" : False
    }
    ,"CRT_DT_HDR" : {
      "expr" : ( col("vbak.ERDAT") )
      ,"dependentOnTables" : [ "vbak" ]
      , "requiredC" : False
    }
    ,"CR_CHK_TOT_STS_CD" : {
      "expr" : ( col("vbuk.CMGST") )
      ,"dependentOnTables" : [ "vbuk" ] 
      , "requiredC" : False
    }
  }

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
  
  print( requiredColumnsSelectExprList  )
  printTableInfoDictStatus( tablesInfoDict )
 
  # Step 3- To join the only required tables and keep it in a intermediate expression
  startingTableKey = None   #Either we can write it manually otherwise we can generate from info Dict... Info dict is seeming better to extract from 
  for tabNameKey in tablesInfoDict:
    if tablesInfoDict[tabNameKey]["isStartingTable"]:
      startingTableKey = tabNameKey
      break
  
  requiredTableJoinExpression = ( tablesInfoDict[startingTableKey]["dataframeVar"].alias( tablesInfoDict[startingTableKey]["alias"]  ) 
                                 )  #will start from the starting table
 
  #Will iterate for other required tables join
  for tabNameKey in tablesInfoDict:
    if (startingTableKey == tabNameKey) or ( not tablesInfoDict[tabNameKey]["requiredT"] ):
      continue #As starting table is already in the join expression and joins for not required columns is not needed
    
    currentTableInfoDict = tablesInfoDict[ tabNameKey ]
    currentTableDataFrameAlongWithAlias  = tablesInfoDict[tabNameKey]["dataframeVar"].alias( tablesInfoDict[tabNameKey]["alias"]  ) 
    requiredTableJoinExpression = requiredTableJoinExpression.join(other= currentTableDataFrameAlongWithAlias, on= currentTableInfoDict["joinConditionExpresion"] , how=currentTableInfoDict["joinType"])

  
  resultDf = requiredTableJoinExpression.select( *requiredColumnsSelectExprList )
  return resultDf


# COMMAND ----------

# heheDf = newApproach( allColumns =  True )

# COMMAND ----------

selectedColDf = newApproach( columnListToSelect =  ["CRT_DT_ITM", "CRT_DT_HDR"] )
selectedColDf.display()

# COMMAND ----------

#Todo: 
# is it better to convert these two classes. One will be ClassInfo and another will be ColumnInfo
# How to do left anti joins if required because there would no columns being selected from here, in the function will we make an argument mustIncludeTableInJoinList and pass the tables key to join those tables mandatoryly for sure. eta ku emiti bi ame kahi pariba ki kichi table ra join karibaku chahunche even thought kichi column naa asu
# Auu jau sequence no darkar padiba ki kichi join taa upare hei thiba darkar
# Kau column paen jadi kichi window function herika darkar thiba tahele upare gote global variable banei use kari paribu
# 

# I know at this point of time it won;t be made generic for all possible scenarios but it's fine gudu if it help one person or only me also then also it is fine, no problem in it

# COMMAND ----------

spark.read.table.

# COMMAND ----------

for x in t.keys():
  print( x )

# COMMAND ----------

for y in t:
  print( y )

# COMMAND ----------

tdf = spark.sql("select 1")
bdf = spark.sql("select 1")


# COMMAND ----------

tdf.join( other= bdf, on=lit(True), how="left" )

# COMMAND ----------


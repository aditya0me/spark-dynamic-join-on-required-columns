# Databricks notebook source
class ValidatingColumnsInfoDict:
  @classmethod
  def checkIfAllDependentOnTableKeysExistInTablesInfoDict(cls, tablesInfoDict, columnsInfoDict):
    passFlag = True

    tableKeysAvailableInTabInfo = tablesInfoDict.keys()
    for columnNameKey in columnsInfoDict:
      for depenedentOnTableKey in columnsInfoDict[columnNameKey]["dependentOnTables"]:
        if depenedentOnTableKey not in tableKeysAvailableInTabInfo:
          print( f"Validation Fail: tableKey={depenedentOnTableKey}, which is present in dependentOnTables list of column {columnNameKey}, does not exist in tables info dictionary")
          passFlag = False
    
    if not passFlag:
      raise Exception("Failed validation in ValidatingColumnsInfoDict.checkIfAllDependentOnTableKeysExistInTablesInfoDict")
    else:
      print("passed in ValidatingColumnsInfoDict.checkIfAllDependentOnTableKeysExistInTablesInfoDict")

  
  @classmethod
  def runValidation(cls, tablesInfoDict, columnsInfoDict):
    cls.checkIfAllDependentOnTableKeysExistInTablesInfoDict( tablesInfoDict, columnsInfoDict )
    
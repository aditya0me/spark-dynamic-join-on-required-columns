# Databricks notebook source
class ValidatingTablesInfoDict:

  @classmethod
  def checkIfAllJoinDependentOnTablesKeyExist(cls, tablesInfoDict ):
    passFlag = True

    allTableKeysPresent = tablesInfoDict.keys()
    # notPresentTabKeysList = []
    for tabKey in allTableKeysPresent:
      joinDependentOnTablesList = tablesInfoDict[tabKey]["joinDependentOnTables"]
      for dependentOnTabKeyItr in joinDependentOnTablesList:
        if dependentOnTabKeyItr not in allTableKeysPresent:
          print( f"Validation Fail:tableKey={dependentOnTabKeyItr}, which is present in joinDependentOnTablesList of {tabKey}, does not exist in tables info dictionary")
          passFlag = False

    if not passFlag:
      raise Exception("Failed validation in ValidatingTablesInfoDict.checkIfAllJoinDependentOnTablesKeyExist function")
    else:
      print("passed in checkIfAllJoinDependentOnTablesKeyExist")
  
  @classmethod
  def checkIfTablesJoinHaveUniqueSequenceNo(cls, tablesInfoDict):
    passFlag = True
    sequenceNoOfTablesJoin_set = set()
    for tabKey in tablesInfoDict:
      currentTabKeySeqNo = tablesInfoDict[tabKey]["tableJoinSeq"]
      if not isinstance( currentTabKeySeqNo, int ):
        print(f"Validation Fail: seqNo of tableKey={tabKey} should be of an integer type. Currently it is {currentTabKeySeqNo} of type{currentTabKeySeqNo}")
        raise Exception("Failed validation in ValidatingTablesInfoDict.checkIfTablesJoinHaveUniqueSequenceNo function")

      if currentTabKeySeqNo in sequenceNoOfTablesJoin_set:
        print( f"""Validation Fail:{currentTabKeySeqNo} is present multiple times""" )
        passFlag = False
      else:
        sequenceNoOfTablesJoin_set.add( currentTabKeySeqNo )
  
    if not passFlag:
      raise Exception("Failed validation in ValidatingTablesInfoDict.checkIfTablesJoinHaveUniqueSequenceNo function")
    else:
      print("passed in ValidatingTablesInfoDict.checkIfTablesJoinHaveUniqueSequenceNo")
  
  @classmethod
  def checkThereShouldBeOneStartingTable( cls, tablesInfoDict ):
    countOfTabKeyWithStartingTableEnabled = 0
    for tabKey in tablesInfoDict:
      if tablesInfoDict[tabKey]["isStartingTable"]:
        countOfTabKeyWithStartingTableEnabled += 1

    if countOfTabKeyWithStartingTableEnabled == 0:
      raise Exception("Failed validation in ValidatingTablesInfoDict.checkThereShouldBeOneStartingTable function. There is no tables with isStartingTable value enabled")
    elif countOfTabKeyWithStartingTableEnabled > 1:
      raise Exception(f"Failed validation in ValidatingTablesInfoDict.checkThereShouldBeOneStartingTable function. {countOfTabKeyWithStartingTableEnabled} tables having isStartingTable value enabled. There should be single starting table.")
    else:
      print("passed in ValidatingTablesInfoDict.checkThereShouldBeOneStartingTable")

  @classmethod
  def checkStartingTableIsWithMinimumJoinSeqNo(cls, tablesInfoDict):
    minimumTableJoinSeqNo = 999999 #Initializing with fairly large number
    startingTableJoinSeqNo = None
    for tabKey in tablesInfoDict:
      currentTabKeySeqNo = tablesInfoDict[tabKey]["tableJoinSeq"]
      minimumTableJoinSeqNo = min(minimumTableJoinSeqNo, currentTabKeySeqNo)
      if tablesInfoDict[tabKey]["isStartingTable"]:
        startingTableJoinSeqNo = currentTabKeySeqNo
    
    if startingTableJoinSeqNo != minimumTableJoinSeqNo:
      raise Exception(f"Failed validation in ValidatingTablesInfoDict.checkStartingTableIsWithMinimumJoinSeqNo function. Starting table sequence is not minimum amongst all.")
    else:
      print("passed in ValidatingTablesInfoDict.checkStartingTableIsWithMinimumJoinSeqNo")
  
  @classmethod
  def checkIfATablesJoinDependentTableAreWithLowerSequenceNo(cls, tablesInfoDict):
    #If we will closely observe this check also satisfies the checking "if there is any cycles exists for the join dependent tables". By looking this requirement first thought would came to make a graph data structure using joinDependentOnTable recursively and check presence of any cycle in the graph. But what i have a strong feeling ki, this current function check is indirectly checking presence of cycles also. So if you will think of joining multiple tables, they would be joined one after other right, so they can have increasing order of sequence-no for joining. So in tablesInfoDict, if every table's join is either not dependent or dependent on tables with lesser sequence no there won't be chance of presence of cycle, also we no need to check recursively for a tables joinDependentOnTable and their joinDependentTables, because they will be individually checked any way.  

    passFlag = True
    for tabKey in tablesInfoDict:
      currentTableKeySeqNo = tablesInfoDict[tabKey]["tableJoinSeq"]
      currentTableJoinDependsOnList = tablesInfoDict[tabKey]["joinDependentOnTables"]
      if len(currentTableJoinDependsOnList) == 0: #For starting table and some join condition putting like lit(True), those cases there won't be joinDependentOnTable
        continue
      
      joinDependsOnTablesSeqNoList = [tablesInfoDict[itr]["tableJoinSeq"] for itr in currentTableJoinDependsOnList ]
      if currentTableKeySeqNo <= max( joinDependsOnTablesSeqNoList ):
        print(f"""Validation Fail: table={tabKey} 's joinDependentOnTables list has tables with sequence no greater than {tabKey} 's sequence no""")
        passFlag = False


    if not passFlag:
      raise Exception(f"Failed validation in ValidatingTablesInfoDict.checkIfATablesJoinDependentTableAreWithLowerSequenceNo function.")
    else:
      print("passed in ValidatingTablesInfoDict.checkIfATablesJoinDependentTableAreWithLowerSequenceNo")



  @classmethod
  def runValidation( cls, tablesInfoDict ):
    cls.checkIfAllJoinDependentOnTablesKeyExist( tablesInfoDict )
    cls.checkIfTablesJoinHaveUniqueSequenceNo( tablesInfoDict )
    cls.checkThereShouldBeOneStartingTable( tablesInfoDict )
    cls.checkStartingTableIsWithMinimumJoinSeqNo( tablesInfoDict )
    cls.checkIfATablesJoinDependentTableAreWithLowerSequenceNo( tablesInfoDict )


    # pass

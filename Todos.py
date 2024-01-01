# Databricks notebook source
#Todo: 
-> is it better to convert these two classes. One will be ClassInfo and another will be ColumnInfo
Kau column paen jadi kichi window function herika darkar thiba tahele upare gote global variable banei use kari paribu

-> ways to handle cross join of tables, it can be handled what i feel, it's an feature for later.
  -> if i implement it, then the validation function also will have to handled it

-> I know at this point of time it won;t be made generic for all possible scenarios but it's fine gudu if it help one person or only me also then also it is fine, no problem in it. 

-> make various validation functions for the tablesInfoDict and columnsInfoDict. For example 
  ->
Below i don't have that knowledge to do
-> join type string should be on of the valid strings, should we use the enum class here or it will be over engineering for now 
-> Table ra join condition expresssion kimba column expression re emiti kichi alias use heichi ki jauta tablesInfoDict re hi nahi. i don;t know right now how to extract the alias names from the pyspark expression or not. If i can extract it i can also 

# COMMAND ----------

# DBTITLE 1,Todo - Half done

-> Todo Decide what to do: why deep copy is needed and a necessity to help clients if they call with the same instance of tablesInfoDict or columnsInfoDict it will keep the state of last run. Not sure on it what to do, because doing deep copy will be an overhead. Ans - Not possible. Failing with the error " It appears that you are attempting to reference SparkContext from a broadcast variable, action, or transformation. SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063."
  -> so have to warn the client or users who will use it... and also for safety can do meta programming and when tableInfoDict or columnInfoDict is passed to function, in the top we can add a flag kind of isAlreadyUsed and suppose next time same thing is passed we can throw an error saying it is already used, kindly generate an new instance of tablesInfoDict, columnsInfoDict
  -> import copy
  -> tablesInfoDict = copy.deepcopy( tablesInfoDict )
  -> columnsInfoDict = copy.deepcopy( columnsInfoDict )

# COMMAND ----------

# DBTITLE 1,Todo - done

-> Auu jau sequence no darkar padiba ki kichi join taa upare hei thiba darkar - Ans - Haa darkar, karana join kala bele jadi au gote table ra column darkar taa se table taa agge expression re thiba darkar
-> How to do left anti joins if required because there would no columns being selected from here, in the function will we make an argument mustIncludeTableInJoinList and pass the tables key to join those tables mandatoryly for sure. eta ku emiti bi ame kahi pariba ki kichi table ra join karibaku chahunche even thought kichi column naa asu - Ans - function re gote argument rakha heichi 

->make various validation functions for the tablesInfoDict and columnsInfoDict. For example 
  -> check if all the tabKeys should be present that are present in dependent tables list tablesInfoDict and columnsInfoDict
  -> check if all tables have unique join sequence no
  -> check starting table should have minimum join sequence no 
  -> one which checks there should be one and only starting_table, and starting table sequence no should be the minimum sequence no out of all
  -> This two cases i have strong feeling ki, the second check will satisfy the fist check indirectly
    -> one which checks if there is any cycles exists for the join dependent tables
    -> seqNo gote pare gote thiba darkar, for example staring table ra seqNo 1 ki 0 haba katha, au suppose gote table ra join bele tara joining condition au gote table ra column upare depend kare tebe se jau second table ra seqNo current table thu purbaru thiba darkar nahele expression generate kala bele fail heijiba. And also it should not be null. and no two tables should have same sequence no

# COMMAND ----------

# DBTITLE 1,Blog notes
Title
-------
Dynamically join only required tables depending upon the columns needed. 
Benifits
--------
  -> The business logic will be kept at once place so less repeating the code
  -> Increased performance as only required tables will be read and joined


Leverage the flexibility of pyspark api to perform dynamic join. 

Jau dictionary re each variable ra meaning bujhei dabu au function argument re kn kn patha hauchi seta kemit use haba bujhei dabu

Taa pare code ra github link au databricks ra dbc format re extract kari rakhi debu

# from dolphindb import *
import dolphindb as ddb

HOST = "localhost"
PORT = 8801
s = ddb.session(HOST, PORT,"admin","123456")
WORK_DIR = "C:/Tutorials_EN/data"

trade=s.loadText(WORK_DIR+"/example.csv")

# convert the imported DolphinDB table object into a pandas DataFrame
df = trade.toDF()


if s.existsDatabase(WORK_DIR+"/valuedb"):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
trade = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")
print(trade.toDF())

trade = s.loadTable(tableName="trade", dbPath=WORK_DIR + "/valuedb")

s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="")

# "dbPath='db'" means that the system uses database handle 'db' to import data into in-memory partitioned table trade
trade=s.loadTextEx(dbPath="db", partitionColumns=["TICKER"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.rows)
print(trade.cols)
print(trade.schema)

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", partitions=["NFLX","NVDA"], memoryMode=True)

#show how many rows in the table
print(trade.rows)

import os
if s.existsDatabase(WORK_DIR+"/valuedb"  or os.path.exists(WORK_DIR+"/valuedb")):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database(dbName='db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
t = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")

trade = s.loadTableBySQL(tableName="trade", dbPath=WORK_DIR+"/valuedb", sql="select * from trade where date>2010.01.01")
print(trade.rows)

trade=s.ploadText(WORK_DIR+"/example.csv")
print(trade.rows)

import pandas as pd
import numpy as np

df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))


trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", partitions="AMZN")
print(trade.rows)

dt = s.table(data={'id': [1, 2, 2, 3],
                   'ticker': ['APPL', 'AMNZ', 'AMNZ', 'A'],
                   'price': [22, 3.5, 21, 26]}).executeAs("test")

# load table "test" on DolphinDB server(just uploaded from Python)
print(s.loadTable("test").toDF())

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
top10 = trade.top(10).executeAs("top10")
trade.append(top10)
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.rows)

trade=s.loadText(WORK_DIR+"/example.csv")
top10 = trade.top(10).executeAs("top10")
t1=trade.append(top10)

print(t1.rows)

trade = s.loadTable(tableName="trade", dbPath=WORK_DIR+"/valuedb", memoryMode=True)
trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
t1=trade.where("ticker=`AMZN").where("VOL=999999")
print(t1.toDF())

trade = s.loadTable(tableName="trade", dbPath=WORK_DIR+"/valuedb", memoryMode=True)
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

trade = s.loadTable(tableName="trade", dbPath=WORK_DIR + "/valuedb", memoryMode=True)
t1=trade.drop(['ask', 'bid'])
print(t1.top(5).toDF())

# s.dropTable(WORK_DIR + "/valuedb", "trade")
# trade = s.loadTable(tableName="trade", dbPath=WORK_DIR + "/valuedb", memoryMode=True)

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", memoryMode=True)
print(trade.select(['ticker','date','bid','ask','prc','vol']).toDF())


# trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", memoryMode=True)
# print(trade.where('TICKER=`AMZN').where('date>2016.12.15').sort('date').exec('prc'))
# print(trade.where('TICKER=`AMZN').where('date>2016.12.15').exec('count(prc)'))

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", memoryMode=True)

# save filtered result to DolphinDB server variable "t1" through function "executeAs"
t1=trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("t1")
print(t1.toDF())
print(t1.rows)

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select("ticker, date, vol").where("bid!=NULL, ask!=NULL, vol>50000000").toDF())

trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select('ticker').groupby(['ticker']).count().sort(bys=['ticker desc']).toDF())

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,on=["TICKER","date"]).toDF())


trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print("left left left")
print(trade.merge(t1,how="left", left_on=["TICKER","date"]).select("*").toDF())

t2 = s.table(data={'TICKER': ['NFLX', 'NFLX', 'NFLX'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [892, 185, 1000]})
print("outer outer outer")
print(t1.merge(t2,how="outer", on=["TICKER","date"]).select("*").toDF())


# HOST = "localhost"
# PORT = 8801
# s = ddb.session()
# s.connect(HOST, PORT,"admin","123456")
# if s.existsDatabase("dfs://valuedb"):
#     s.dropDatabase("dfs://valuedb")
# s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
# trade=s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', filePath=WORK_DIR + "/example.csv")
# print(trade.rows)
# s.dropPartition("dfs://valuedb", partitionPaths=["/AMZN", "/NFLX"])
# trade = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
# print(trade.rows)
# print(trade.select("distinct TICKER").toDF())

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
t1 = s.table(data={'TICKER1': ['AMZN', 'AMZN', 'AMZN'], 'date1': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,left_on=["TICKER","date"], right_on=["TICKER1","date1"]).select("*").toDF())
import unittest
import os
import sys
import dolphindb as ddb
from datetime import datetime
sys.path.append('..')
from dolphindb import *
from xxdb_server import HOST, PORT
rs = lambda s: s.replace(' ', '')
s = ddb.session(HOST, PORT)
s = session()
s.connect(HOST,PORT,"admin","123456")
WORK_DIR = "C:/Tutorials_EN/data"

# print("test loadText")
# trade = s.loadText(filename=WORK_DIR+"/example.csv")
# print(trade.select("distinct TICKER").toDF())
# #print(trade.toDF())
# print(trade.select("min(date) as date").toDF())
# # 2
# print("test loadText parallel mode")
# trade = s.ploadText( WORK_DIR+"/example.csv")
# print(trade.rows)

# 3
print("test drop & create partitioned database; loadTextEx")
if s.existsDatabase(WORK_DIR+"/valuedb"  or os.path.exists(WORK_DIR+"/valuedb")):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database(dbName='db', partitionType=VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
t = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")
print(t.top(5).toDF())
print(t.where('TICKER=`AMZN').where('date>2016.12.15').sort('date').selectAsVector('prc'))

trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", memoryMode=True)
print(trade.select(['ticker','date','bid','ask','prc','vol']).top(5).toDF())

# print(trade.select("ticker, date, vol").toDF())
# df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").contextby('ticker').top(5).toDF()
# print(df)
# df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").contextby('ticker').having(" sum(VOL)>40000000000").toDF()
# print(df)
# t= s.loadText(filename=WORK_DIR+"/example.csv")
# wjt = t.merge_window(t, -10, -5, aggFunctions='<sum(VOL)>', on=["TICKER","date"], prevailing=True).toDF()
# print(wjt)
#
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# # c1 = trade.rows
# # print (c1)
# # top10 = trade.top(10).executeAs("top10")
# #
# # c2 = trade.append(top10).rows
# # print (c2)
# # trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# # print(trade.rows)
#
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# t1=trade.where("ticker=`AMZN").where("VOL=999999")
# print(t1.toDF())
#
#
# print("here")
#
# #4 To load only the "AMZN" partition:
# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", partitions="AMZN")
# print(trade.rows)
#
# #5 To load "NFLX" and "NVDA" partitions as an in-memory partitioned table:
#
# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", partitions=["NFLX","NVDA"], memoryMode=True)
# print(trade.rows)

# #6 We can use a list of field names in **select** method to select columns. We can also use **where** method to filter the selection.
#
# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", memoryMode=True)
# print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').top(5).toDF())
# print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())

#7

print(trade.select("ticker, date, vol").where("bid!=NULL, ask!=NULL, vol>50000000").top(5).toDF())

#8

print(trade.select('ticker').groupby(['ticker']).count().sort(bys=['ticker desc']).toDF())

#9

print(trade.select(['vol','prc']).groupby(['ticker']).sum().toDF())

#10

print(trade.select('count(ask)').groupby(['vol']).having('count(ask)>1').toDF())

#11

df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").select(["vol","prc"]).contextby('ticker').sum().top(10).toDF()
print(df)

#12

trade = s.loadTable(dbPath=WORK_DIR + "/valuedb", tableName="trade", memoryMode=True)
z=trade.select(['bid','ask','prc']).ols('PRC', ['BID', 'ASK'])
print(z['ANOVA'])
print(z["RegressionStat"])
print(z["Coefficient"])
print(z["Coefficient"].beta[1])

#13

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,on=["TICKER","date"]).select(["date","prc","open"]).toDF())
# print(trade.merge(t1,on=["ticker","date"]).toDF())

#14
from datetime import datetime
dates = [Date.from_date(x) for x in [datetime(1993,12,31), datetime(2015,12,30), datetime(2015,12,29)]]
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': dates, 'open': [695, 685, 674]})
print(trade.merge_asof(t1,on=["date"]).top(20).toDF())

#15

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
t1=trade.where("ticker=`AMZN").where("VOL=999999")
print(t1.count())

# 16

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

#17
trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
t1=trade.drop(['ask', 'bid'])
print(t1.top(5).toDF())

#18
trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
t1=trade.drop('ask')
print(t1.top(5).toDF())

#19

s.dropTable(WORK_DIR + "/valuedb", "trade")

#20

trade=s.ploadText( WORK_DIR+"/example.csv")
print(trade.toDF())

#21
s.database('db',partitionType=VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="")
trade=s.loadTextEx(dbPath="db", partitionColumns=["TICKER"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.rows)

#
if s.existsDatabase(WORK_DIR+"/valuedb"  or os.path.exists(WORK_DIR+"/valuedb")):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database(dbName='db', partitionType=VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
t = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")
trade = s.loadTableBySQL(dbPath=WORK_DIR + "/valuedb",tableName="trade", sql="select * from trade where date>2010.01.01")
print(trade.rows)
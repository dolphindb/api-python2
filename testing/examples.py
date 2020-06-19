import unittest
import os
import sys
import dolphindb as ddb
from datetime import datetime
sys.path.append('..')
from dolphindb import *
# from xxdb_server import HOST, PORT
rs = lambda s: s.replace(' ', '')
HOST = "localhost"
PORT = 8080
s = ddb.session(HOST, PORT)
s = session()
s.connect(HOST,PORT,"admin","123456")
WORK_DIR = "C:/Tutorials_EN/data"


s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="")
trade=s.loadTextEx(dbPath="db", partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.toDF())
#
print(s.run('wavg( [100, 60, 300], [1, 1.5, 2])'))
#
# v1=s.run("v1=3 1 2 5 7; sort v1")
# print(v1)
#
# s.run("t1=table(1 2 3 as id, 4 5 6 as v)")
# t=s.run("select * from t1")
# print(t)
#
# df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
# s.upload({'t1': df})
# print(s.run("t1.value.avg()"))
#
#
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# t = s.run("select bid, ask, prc from trade where bid!=NULL, ask!=NULL, vol>1000")
# print(t)
#1
print("test loadText")
trade = s.loadText(filename=WORK_DIR+"/example.csv")
print(trade.select("distinct TICKER").toDF())
#print(trade.toDF())
print(trade.select("min(date) as date").toDF())
# 2
print("test loadText parallel mode")
trade = s.ploadText( WORK_DIR+"/example.csv")
print(trade.rows)




# 3
print("test drop & create partitioned database; loadTextEx")
if s.existsDatabase(WORK_DIR+"/valuedb"  or os.path.exists(WORK_DIR+"/valuedb")):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database(dbName='db', partitionType=VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
t = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")
print(t.top(5).toDF())
print(t.where('TICKER=`AMZN').where('date>2016.12.15').sort('date'))

trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", memoryMode=True)
print(trade.select(['ticker','date','bid','ask','prc','vol']).top(5).toDF())

print(trade.select("ticker, date, vol").toDF())
df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").contextby('ticker').top(5).toDF()
print(df)
df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").contextby('ticker').having(" sum(VOL)>40000000000").toDF()
print(df)



df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").select("TICKER, month(date) as month, cumsum(VOL)").contextby("TICKER,month(date)").top(10).toDF()
print(df)

df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").select("TICKER, month(date) as month, sum(VOL)").contextby("TICKER,month(date)").top(5).toDF()
print(df)

t= s.loadText(filename=WORK_DIR+"/example.csv")
wjt = t.merge_window(t, -10, -5, aggFunctions='sum(VOL)', on=["TICKER","date"], prevailing=True).toDF()
print(wjt)

trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# c1 = trade.rows
# print (c1)
# top10 = trade.top(10).executeAs("top10")
#
# c2 = trade.append(top10).rows
# print (c2)
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# print(trade.rows)

# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# t1=trade.where("ticker=`AMZN").where("VOL=999999")
# print(t1.toDF())


print("here")
#4 To load only the "AMZN" partition:
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", partitions="AMZN")
print(trade.rows)

#5 To load "NFLX" and "NVDA" partitions as an in-memory partitioned table:

trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", partitions=["NFLX","NVDA"], memoryMode=True)
print(trade.rows)


#6 We can use a list of field names in **select** method to select columns. We can also use **where** method to filter the selection.

trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade", memoryMode=True)
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').top(5).toDF())
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())


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
print(t1.rows)

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
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# # trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")
# # t1 = s.table(data={'sym': ['AMZN', 'NFLX', 'NVDA'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [7.1, 7.5, 7.3]})
# # print("test merge")S
# # print(trade.merge(t1,on=["sym","date"]).toDF())
#
#
# # 4. loadTableBySQL
# print("test loadTableBySQL")
# trade = s.loadTableBySQL(dbPath=WORK_DIR + "/valuedb", tableName="trade", sql = "select * from trade where date > 2010.01.01")
# print(trade.rows)
#
# # 5 test top
# print("test select")
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# t1=trade.select(['bid','ask','prc','vol']).where('bid!=NULL').where('ask!=NULL').where('vol>1000')
# print(t1.count())
# # print (trade.top(5).toDF())
# #
# #
# # 6 update
# print("test update")
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# trade = trade.update(["VOL"],["200"]).where(["date>1992.01.14"]).execute().where("VOL=200")
# t1=trade.where("VOL=200")
# print(t1.count())
# print(trade.rows)
#
#
# # # 7 top
# print("test top 10")
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# # print(trade.top(10).count())
# #
# # # 8 agg2
# print(trade.groupby('sym').agg2('wavg',('prc','vol')).toDF())
#
#
#
# #8 executeAs: assign a subset to a new table
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# top10 = trade.top(10).executeAs("top10")
# print(top10.toDF())
# #
#
# # 'int' object is not callable
# # # 9 count num of records
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# num = trade.rows
# print(num)
# #
# #
# # 10 append
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# c1 = trade.rows
# print (c1)
# top10 = trade.top(10).executeAs("top10")
# c2 = trade.append(top10).count()
# print (c2)
# assert c1 + 10 == c2
#
#
# # 11 delete
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# top10=trade.where('date<1992.01.15').delete().execute().top(10).executeAs("top10")
# print (top10.toDF().min())
#
# # 12 drop drop column
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# trade.drop(['ask', 'prc'])
# print(trade.top(10).toDF())
#
# #13 group by
# print("group by")
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# print(trade.select('prc').groupby(['sym']).count().sort(bys=['sym desc']).toDF())
# print(trade.select(['vol','prc']).groupby(['sym']).sum().toDF())
#
# #14 group by having
# print("group by having")
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# print(trade.select('count(ask)').groupby(['VOL']).having('count(ask)>15').toDF())
#
#
# #15 ols
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# # print(trade.select(['bid','ask','prc']).ols('PRC', ['BID', 'ASK']))
# z=trade.select(['bid','ask','prc']).where('bid!=NULL').where('ask!=NULL').ols('PRC', ['BID', 'ASK'])
# print(z.keys())
# print(z["ANOVA"])
# print(z["RegressionStat"])
# print(z["Coefficient"])
# #16 columns, count, schema
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# print(trade.columns)
# print(trade.rows)
# print(trade.schema)
#
# #17 select as vector
#
# trade = s.table(dbPath=WORK_DIR + "/valuedb", data="trade", inMem=True)
# v1=trade.selectAsVector("prc")
# print(v1)
#
# #### DFS
#
# # 1 create db
#
# if s.existsDatabase("dfs://valuedb"):
#     s.dropDatabase("dfs://valuedb")
# s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://valuedb")
# trade = s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
# print(trade.rows)
#
#
#
# # t1 = s.table(data="t1", dbPath="dfs://test1")
# # print(t1.toDF())
# # 1
# # quotes = loadTable("dfs://TAQ", "quotes")
# # t1 = s.table(data="quotes", dbPath="dfs://TAQ")
#
# # 2
# # select count(*) from quotes
# df = t1.select("count(*)").toDF()
# print(df)
#
# # 3
# # x=select symbol, time, bid, ofr from quotes where symbol='LEH', date=2007.08.21, time=15:59:00
# df = t1.select([ "ask", "bid", "prc"]).where("sym='EWST'").where("date=2007.08.21").where("ask=13.9").toDF()
# print(df)
#
# # 4
# # avgSpread = select avg((ofr-bid)/(ofr+bid)*2) as avgSpread from quotes where date=2007.08.31, time between 09:30:00 : 15:59:59, ofr>bid, ofr>0, bid>0, ofr/bid<1.2 group by minute(time) as minute
# df = t1.select("avg((prc-bid)/(prc+bid)*2) as avgSpread").where("date=2007.08.31, time between 09:30:00 : 15:59:59, prc>bid, prc>0, bid>0, prc/bid<1.2").groupby("bid").toDF()
# print(df)
#
# #5
# # select avg((ofr-bid)/(ofr+bid)*2) as avgSpread from quotes where date=2007.09.25, symbol=`LEH, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2 group by minute(time) as minute
# df = t1.select("avg((prc-bid)/(prc+bid)*2) as avgSpread").where("date=2007.09.25, sym='EWST', time between 09:30:00 : 15:59:59, prc>bid, bid>0, prc/bid<1.2").groupby("bid").toDF()
# print(df)
#
# # 6
# #select last((ofr+bid)/2) as last from quotes where date<2007.09.01, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2 group by symbol, date, minute(time) as minute
# df = t1.select("last((ofr+bid)/2) as last").where("date<2007.08.03, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2").groupby(["symbol", "date", "minute(time)"]).toDF()
#
# # # 7
# # "/data/ssd/DolphinDBDemo/EQY"
# df= s.table(data="trade", dbPath="dfs://valuedb").select(["vol","prc"]).where("vol>500").contextby('sym').sum().top(10).toDF()
# print(df)

# # 8
# df = s.table(data="trade", dbPath="dfs://test1").select(["date","sym","vol","prc"]).where("vol>500").contextby('sym').agg({'vol':['date'] }).toDF()
# print(df)
#

# #9
# df= s.table(data="trade", dbPath="dfs://test1").select(["date","sym","vol","prc"]).where("vol>500").contextby('sym').having('count(sym)>3').top(10).toDF()
# print(df)



#part 3
# s.database('db',partitionType=RANGE, partitions=[10000,20000,30000,40000,50000,60000,70000,80000,90000,100000], dbPath=WORK_DIR+"/USsample")
# t1=s.loadTextEx(dbPath=WORK_DIR+"/USsample", partitionColumns=["PERMNO"], tableName='USsample', filePath=WORK_DIR + "/example.csv")
# print(t1.count)

#1
# s.database('db',partitionType=RANGE, partitions=["EGAS","EWST","GFGC"], dbPath=WORK_DIR+"/USsample1")
# t1=s.loadTextEx(dbPath=WORK_DIR+"/USsample1", partitionColumns=["sym"], tableName='USsample', filePath=WORK_DIR + "/example.csv")
# print(t1.count())

#2  ????????? no inMemPartitions
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", inMem=True, inMemPartitions=["sym"])
# print(trade.showSQL())

#3 ---------
# t1 = s.loadTableBySQL(dbPath=WORK_DIR+"/USsample1", tableName="USsample", sql="select ask from USsample").toDF()
# print(t1)

#4
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", inMem=True)
# trade.update(["PRC", "VOL"],["PRC+1", "200"]).where(["SYM='GFGC'"]).execute()
# print(trade.top(10).toDF())

#5
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", inMem=True)
# print(trade.rows)

#6
# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade", inMem=True)
# top10 = trade.top(10).executeAs("top10")
# print(trade.append(top10).count())



#7
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", inMem=True)
# trade.update(["PRC", "VOL"],["PRC+1", "200"]).where(["SYM='GFGC'"]).execute()
# print(trade.showSQL())
# #
# print(trade.toDF())
# print(trade.where(["SYM='EGAS'"]).toDF())

#8
# make table before test！！！！！！！！！！！！！！！！！
# s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://US")
# trade = s.loadTextEx(dbPath="dfs://US", partitionColumns=["sym"], tableName='US', filePath=WORK_DIR + "/example.csv")
#
# # s = session()
# # s.connect('38.124.1.173', 8921)
# US = s.table(dbPath="dfs://US", data="US")
# print(US.select('max(PRC)').where('VOL>0').groupby(['date','ask']).toDF())


#9
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", partitions=["GFGC","EWST", "EGAS"])
# print(trade.toDF())

#10
# US = s.table(dbPath="dfs://US", data="US")
# print(US.select('max(PRC)').where(['date>2008.11.30','ask>8']).groupby(['date','ask']).toDF())

#11
# trade = s.table(dbPath=WORK_DIR+"/USsample1", data="USsample", inMem=True)
# trade.update(["PRC", "VOL"],["PRC+1", "200"]).where(["SYM='EGAS'"]).execute()

# #12
# trade = s.table(dbPath=WORK_DIR + "/USsample1", data="USsample", inMem=True)
# trade.drop(['prc', 'vol'])
# print(trade.toDF())
#
# #13
# trade = s.table(dbPath=WORK_DIR + "/USsample1", data="USsample", inMem=True)
# trade.drop(['bid', 'vol'])
# print(trade.top(10).toDF())


#
# s = session()
# s.connect('38.124.1.173', 8921)
# s.run('dateValue=2007.08.01')
# s.run('num=500')
# quotes = s.table(data='quotes', dbPath="dfs://TAQ")
# quotes.select("count(*)").where(['date=dateValue','time between 09:30:00 : 15:59:59', 'bid>0','bid<ofr','ofr<bid*12.']).groupby('symbol').sort('count desc').executeAs('symCount')
# symCount = s.table(data="symCount")
# syms = symCount.selectAsVector('symbol', vectorAlias="syms")
# quotes.where("date = dateValue, Symbol in syms, 0<bid, bid<ofr, ofr<bid*1.2, time between 09:30:00 : 15:59:59 ").pivotby("time.minute() as minute", "symbol", "avg(bid + ofr)/2.0 as price").selectAsVector(vectorAlias="priceMatrix")
# s.run("retMatrix=each(def(x):ratios(x)-1, priceMatrix)")
# s.run("corrMatrix=cross(corr, retMatrix, retMatrix)")
# s.close()

#df = s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time", "Symbol", "Trade_Volume", "Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').top(10).toDF()
# print s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time", "Symbol", "Trade_Volume", "Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').agg({'trade_volume':[ddb.cumsum] }).toDF()
# print s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time","Symbol","Trade_Volume","Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').agg2('wavg', ('Trade_Price', 'Trade_Volume')).toDF()


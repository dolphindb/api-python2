import dolphindb as ddb
from datetime import *
import pandas as pd
import numpy as np
import time
# conn = ddb.session()
# success = conn.connect("localhost", 8080)
#
# dates = conn.run('2012.10M + rand(100,100)')
# z={'date':dates}
# df=pd.DataFrame(z)
#df['date']=df['date'].astype('datetime64[ns]')
#print(df)

HOST = "localhost"
PORT = 8080
s = ddb.session(HOST, PORT,"admin","123456")
# s.connect(HOST, PORT)
WORK_DIR = "C:/Tutorials_EN/data"

# if s.existsDatabase(WORK_DIR+"/valuedb"):
#     s.dropDatabase(WORK_DIR+"/valuedb")
# s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
# trade = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")


# trade = s.table(dbPath=WORK_DIR+"/valuedb", data="trade")


# print(trade.contextby("ticker").top(5).toDF())
# print(trade.select("top 5 *").contextby("ticker").toDF())
# t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
# print(t1.toDF())


# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
# t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
# print(trade.merge(t1,on=["TICKER","date"]).toDF())

# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
# t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
# print(trade.merge(t1,on=["TICKER","date"]).toDF())
# print(t1.merge(trade,how="left", on=["TICKER","date"]).select("*").toDF())
# try:
#     print(t1.merge(trade,how="left", on=["TICKER","date"]).select("*").toDF())
# except:
#     trade = s.loadTable(dbPath=WORK_DIR + "/valuedb", tableName="trade")
#     t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'],
#                        'open': [695, 685, 674]})
#     print(trade.merge(t1, on=["TICKER", "date"]).toDF())

# trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
# t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date':[date(2015,12,31), date(2015,12,30), date(2015,12,29)], 'open': [695, 685, 674]})
# print(t1.merge(trade,on=["TICKER","date"]).toDF())


# if s.existsDatabase(WORK_DIR+"/NYSE"):
#      s.dropDatabase(WORK_DIR+"/NYSE")
# s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX"], dbPath=WORK_DIR+"/NYSE")
# trades = s.loadTextEx("db",  tableName='trades',partitionColumns=["Symbol"], filePath=WORK_DIR + "/trades.csv")
# quotes = s.loadTextEx("db",  tableName='quotes',partitionColumns=["Symbol"], filePath=WORK_DIR + "/quotes.csv")
# # df = trades.toDF()
# # print(df)
#
#
# #print(trades.merge_window(quotes, -5000000000, 0, aggFunctions=["avg(Bid_Price)","avg(Offer_Price)"], on=["Symbol","Time"]).where("Time>=15:59:59").toDF())
# # print(trades.merge_window(quotes, -5000000000, 0, aggFunctions='avg(Bid_Price)', on=["Symbol","Time"]).top(5).toDF())
# # trades.merge_window(quotes, -1, 0, aggFunctions="[avg(Offer_Price) as Offer_Price, avg(Bid_Price) as Bid_Price]", on=["Symbol","Time"], prevailing=True).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost").groupby("Symbol").toDF()
#
# # print(s.run("true"))
# # print(s.run("true false true"))
# # print(s.run("false")) # char, short, long ->int
# # print(type(s.run("1.52"))) #float, double -> float
# # print(type(s.run("1.52")))
# # print(s.run("1.5 2.6 7.8"))
# # print(s.run("1.5 2.6 7.8")+1)
# # print(type(s.run("`A`B`C")))
#
#
#
# # select sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost from aj(trades,quotes,`Symbol`Time) group by symbol
#
# print(trades.merge_asof(quotes, on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost").groupby("Symbol").toDF())
# trades=s.loadText(WORK_DIR+"/trades.csv")
# quotes = s.loadText(WORK_DIR+"/quotes.csv")
# print(trades.merge_asof(quotes, on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost").groupby("Symbol").toDF())
dt = datetime.now()
dt64=np.datetime64(dt)
print(dt)
print(ddb.NanoTimestamp.from_datetime64(pd.Timestamp(dt64)))
import dolphindb as ddb
from datetime import *

# print( time.time_ns())

import tushare as ts
import datetime
import pandas as pd
import numpy as np
import dolphindb as ddb
pro=ts.pro_api('bd5d5477781fa7164b54bea773556a7475fb990066f8ba5e9c3c51d4')
df=pro.daily(trade_date='20170104')
#df['trade_date']=df['trade_date'].apply(lambda x: pd.Timestamp(x))
df['trade_date']=pd.to_datetime(df['trade_date'])
conn=ddb.session()
conn.connect('localhost',8080,"admin","123456")
print(df.dtypes)
conn.upload({'t':df})
print(conn.run("t"))



# print(ddb.Date.from_date(date.today()))
# print(ddb.Month.from_date(date.today()))
# print(ddb.Datetime.from_datetime(datetime.today()))
# print(ddb.Timestamp.from_datetime(datetime.today()))
# print(ddb.Minute.from_time(datetime.today().time()))
# print(ddb.Second.from_time(datetime.today().time()))
# print(ddb.NanoTime.from_time(datetime.today().time()))
# print(ddb.NanoTimestamp.from_datetime(datetime.today()))
#
# z = {'date':[ddb.Date.from_date(date.today())], 'month': [ddb.Month.from_date(date.today())],
#      'datetime':[ddb.Datetime.from_datetime(datetime.today())],
#      'timestamp':[ddb.Timestamp.from_datetime(datetime.today())],
#      'second': [ddb.Second.from_time(datetime.today().time())],
#      'minute': [ddb.Minute.from_time(datetime.today().time())],
#      'nanotime':[ddb.NanoTime.from_time(datetime.today().time())],
#      'nanotimestamp':[ddb.NanoTimestamp.from_datetime(datetime.today())]
# }
# import pandas as pd
# df = pd.DataFrame(data=z)
# t1 = s.table(data=df)
# df2=t1.toDF()
# print(df2)
# print(df2.dtypes)
# print (type(s.run("19:10:23")))
# t2=t1.append(s.table(data=df))
# print (t2.toDF()
#
#)
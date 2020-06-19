import pandas as pd
import dolphindb as ddb
import numpy as np
df = pd.read_csv("cr_BTC.csv")
df['timestamp']=pd.to_datetime(df['timestamp'], unit='s')
df['date']=pd.to_datetime(df['date'])
s = ddb.session()

s.connect("38.124.1.173",8921, "admin","123456")
trade=s.loadTable(tableName="trade",dbPath="dfs://EQY")
print(trade.rows)

# t=s.loadTable(tableName="coins_rebalance", dbPath="dfs://coins_rebalance")
# for x in range(100000):
#     t.append(s.table(data=df))
#     print(t.rows)
# print(type(ddb.NanoTimestamp.from_datetime64(np.datetime64('1970-01-01T15:31:32.123456789'))))
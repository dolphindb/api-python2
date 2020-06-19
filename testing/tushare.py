import datetime
import numpy as np
import dolphindb as ddb
import tushare as ts

pro=ts.pro_api('2ae57756221025710e0f33971119da2a53e8728761917748750abcc4')
s=ddb.session()
s.connect("38.124.1.171",8802,"admin","123456")
t1=s.loadTable(tableName="hushen_daily_line",dbPath="dfs://tushare")
t2=s.loadTable(tableName="hushen_daily_indicator",dbPath="dfs://tushare")
def dateRange(beginDate,endDate):
    dates=[]
    dt=datetime.datetime.strptime(beginDate,"%Y%m%d")
    date=beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt=dt + datetime.timedelta(1)
        date=dt.strftime("%Y%m%d")
    return dates

for dates in dateRange('20080101','20171231'):
    df=pro.daily(trade_date=dates)
    if len(df):
        t1.append(s.table(data=df))
        print(t1.rows)

for dates in dateRange('20080101','20171231'):
    ds=pro.daily(trade_date=dates)
    ds['volume_ratio']=np.float64(ds['volume_ratio'])
if len(ds):
    t2.append(s.table(data=ds))
    print(t2.rows)
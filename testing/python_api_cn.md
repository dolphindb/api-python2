# DolphinDB Python API

DolphinDB Python API makes it easy to integrate your Python application, library, or script with DolphinDB server. Currently, DolphinDB API supports Python 2.7.

### 1.Connect to DolphinDB

Python interacts with DolphinDB through a session as below. You should start the DolphinDB server before starting the tutorial.

```
import dolphindb as ddb

// create a session to interact with
s = ddb.session()

// connect to DolphinDB through domain name/IP address and port number
s.connect("localhost",8848)

```

When you need to enter your username and password, you can use the following method.

```
s.connect("localhost",8848, YOUR_USER_NAME, YOUR_PASS_WORD)
```


### 2.Import Data as a DolphinDB In-memory Table


User can import a test file into DolphinDB through session method loadText. This function returns a DolphinDB Table object in Python, which corresponds to a in-memory
table "t1" on DolphinDB server.

为了方便学习，本教程提供了一个例子csv文件: [example.csv](data/example.csv). Note that linux style absolute path must be provided in order for DolphinDB server to locate the file.

```
WORK_DIR = "C:/Tutorials_EN/data"
trade=s.loadText("trade", WORK_DIR+"/example.csv")

# import data into Pandas dataframe
df = trade.toDF()
print(df)

# output
           prc    vol    bid   sym        ask        date
0       8.6000  11337   7.75  EGAS  999.98999  2009.08.04
1       8.5780   1201   8.35  EGAS    8.60000  2009.08.05
2       8.7500   2463   8.20  EGAS    8.75000  2009.08.06
3       8.5800  13034   8.06  EGAS    8.62000  2009.08.07
4       8.2600  13891   8.26  EGAS    8.63000  2009.08.10
5       8.2600   6276   8.26  EGAS    8.74000  2009.08.11
...        ...    ...    ...   ...        ...         ...
5444  17.3750    500  16.50  GFGC   17.50000  1993.11.17
5445  17.5000    600  16.50  GFGC   17.50000  1993.11.19

```

The default separator for this function is a comma ",". You can specify other single characters as delimiters such as '\t'. For example, you can import a tabular text file as the following:

```

t1=s.loadText("t1", WORK_DIR+"/t1.tsv", '\t')
```

For fast processing, you can load the text in parallel mode.On the server end, DolphinDB creates a partitioned in-memory table trade.

```
trade=s.loadText("trade", WORK_DIR+"/example.csv", parallel=True)
```


#### 3 将数据加载到分区数据库

通过loadText将数据直接加载到内存，必然受到内存大小的限制，如果需要处理数据量比较大，更好的方法是创建分区数据库和数据表，并将数据追加到数据表中。


#### 3.1 磁盘分区数据表

将数据导入分区表中，可以通过函数loadTextEx。如果数据库已经存在，该函数会根据用户提供的分区表名，在数据库里创建分区表（如果分区表不存在）以及向分区表中追加数据。如果数据库不存在，并且用户需提供了数据库路径，指向该数据库的句柄，以及分区方案，该函数会首先创建一个分区数据库。
在这里dbPath是数据库的路径； dbHandleName是在数据库端指向该数据库的句柄；partitionType是分区类型；partitions定义具体的分区； tradeName是将要创建或者追加数据的分区数据表名；partitionColumns是该分区表的分区字段；filePath是需要导入的文件绝对路径；delimiter是该文件的分隔符，默认为逗号。

下面的loadTextEx函数会做以下三件事情：
* 创建分区数据库valdb
* 在该数据库中，创建分区数据表trade
* 将example.csv中的数据追加到分区表trade

#### 3.1.1 创建数据库

判断数据库是存在，如果存在，删除数据库。

```
if s.existsDatabase(WORK_DIR+"/valdb"):
    s.dropDatabase(WORK_DIR+"/valdb")
```


在本地路径上创建包括三个值的按值分区的数据库。 这里db是服务器端改数据库对应变量名称

```
s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="C:/Tutorials_EN/data/valdb")
```

在dfs上创建分区数据库和本地创建数据库唯一的区别就是路径不一样。
```
s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://valdb")
```

除了VALUE分区之外，DolphinDB支持的其它分区包括:SEQ,RANGE,COMBO,以及HASH。没有一种分区适合所有数据，用户根据自身数据特点选择合适的分区模式。


数据库创建成功之后，可以通过loadTextEx将文本文件导入到分区数据库中，以下例子。导入的数据表名称为"trade", 分区字段名是"sym"。
```
trade=s.loadTextEx(dbPath=WORK_DIR+"/valdb", dbHandleName="valdb",partitionType=VALUE,  partitions=["GFGC","EGAS", "EWST"], tableName="trade",partitionColumns=["sym"],  filePath=WORK_DIR + "/example.csv", delimiter=",")
print(trade.toDF())

# output
          prc    vol    bid   sym        ask        date
0      8.6000  11337   7.75  EGAS  999.98999  2009.08.04
1      8.5780   1201   8.35  EGAS    8.60000  2009.08.05
2      8.7500   2463   8.20  EGAS    8.75000  2009.08.06
3      8.5800  13034   8.06  EGAS    8.62000  2009.08.07
4      8.2600  13891   8.26  EGAS    8.63000  2009.08.10
5      8.2600   6276   8.26  EGAS    8.74000  2009.08.11
...       ...    ...    ...   ...        ...         ...
5444  17.3750    500  16.50  GFGC   17.50000  1993.11.17
5445  17.5000    600  16.50  GFGC   17.50000  1993.11.19

```

如果需要重复使用该数据表可以采用以下方法。

```
 trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade")
```


dbHandleName, partitionType, 以及 partitions是可选参数，如果数据库已经存在, 用户无需提供。假如上面数据库已经存在,上面数据库加载语句可以写作如下。

```
trade = s.loadTextEx(dbPath=WORK_DIR+"/valdb", tableName="trade",partitionColumns=["sym"],  filePath=WORK_DIR + "/example.csv")
```


#### 3.3 内存分区数据表

有的时候为了即利用分区数据库的并行计算，又利用内存的速度优势。我们也可以直接将数据以分区数据表的形式导入内存。

#### 3.3.1 loadTextEx
我们可以使用相同的函数loadTextEx，与磁盘数据表的不同之处在于:1)不用提供数据存储路径;2)需要提供分区类行;3)以及数据库分区方案

```
trade=s.loadTextEx(partitionType=VALUE,partitions=["GFGC","EWST", "EGAS"], partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.toDF())

```

#### 3.3.2 loadTableBySQL
有的时候我们只需要将一部分书库导入内存。

```
 trade = s.loadTableBySQL(dbPath=WORK_DIR+"/valdb", tableName="trade", sql="select * from trade where date>2010.01.01")

 # output
         prc    vol    bid   sym    ask        date
0     10.2500  18500  10.21  EGAS  10.26  2010.01.04
1     10.1900  23200  10.15  EGAS  10.19  2010.01.05
2     10.3100  18700  10.31  EGAS  10.35  2010.01.06
3      9.9600  29200   9.95  EGAS   9.97  2010.01.07
4     10.3400  25100  10.34  EGAS  10.37  2010.01.08
5     10.5000  23100  10.47  EGAS  10.50  2010.01.11
...       ...    ...    ...   ...    ...         ...
1508   7.6000  16000   7.53  EGAS   7.61  2015.12.30
1509   7.4500  46216   7.45  EGAS   7.49  2015.12.31

```


#### 3 数据表查询和计算

DolphinDB的Table类提供了灵活的链式调用的方法来帮助用户生成SQL语句对数据表进行查询。

#### 3.1 Select

**Use list as input**

用户可以传递一个python列表作为选择的字段列表，同时用链式函数调用的方法，在后面叠加where函数来过滤选择。

```
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)

print(trade.select(['bid','ask','prc']).where('bid!=NULL').where('ask!=NULL').where('vol>1000').toDF())

# output
        bid        ask      prc    vol
0      7.75  999.98999   8.6000  11337
1      8.35    8.60000   8.5780   1201
2      8.20    8.75000   8.7500   2463
3      8.06    8.62000   8.5800  13034
4      8.26    8.63000   8.2600  13891
5      8.26    8.74000   8.2600   6276
...     ...        ...      ...    ...
4113  16.50   17.50000  17.5000   2056
4114  16.50   17.50000  17.5000   1560

```


To show the SQL that is used for the query.

```
print(trade.showSQL())
```

**Use string as input**
用户也直接可以将要选择的字段列表作为字符串传进select函数。对于whwere函数也是如此。

```
trade.select("bid, ask, prc").where("bid!=NULL, ask!=NULL, vol>1000").toDF()
```

**Use run method**

当然用户也可以直接写SQL语句，直接调用run方法。假设服务器上，内存中数据表名是"trade:

```
trade = s.run("select bid, ask, prc where bid!=NULL, ask!=NULL, vol>1000")
print(trade.toDF())
```

**Use function top**

Use function top to print the top records in a table

```
print(trade.top(5).toDF())

#output

     prc    vol   bid   sym        ask        date
0  8.600  11337  7.75  EGAS  999.98999  2009.08.04
1  8.578   1201  8.35  EGAS    8.60000  2009.08.05
2  8.750   2463  8.20  EGAS    8.75000  2009.08.06
3  8.580  13034  8.06  EGAS    8.62000  2009.08.07
4  8.260  13891  8.26  EGAS    8.63000  2009.08.10

```

**Export a single column as a vector**

下面语句返回一个numpy array
```
prcArr = trade.selectAsVector('prc')
print(prcArr)

//output
[ 8.6    8.578  8.75  ... 17.5   17.375 17.5  ]
```


#### 3.2 Group by

Group by column "sym"

```
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
print(trade.select('sym').groupby(['sym']).count().sort(bys=['sym desc']).toDF())

//output

    sym  count_prc
0  GFGC        338
1  EWST       3494
2  EGAS       1614

```

Group by column "sym" and take sum of column "vol" and "prc"

```
print(trade.select(['vol','prc']).groupby(['sym']).sum().toDF())

    sym   sum_vol      sum_prc
0  EGAS  48105456  16670.46090
1  EWST  15972810  32570.34188
2  GFGC    506110   4833.25000
```


Group by having

```
print(trade.select('count(ask)').groupby(['VOL']).having('count(ask)>15').toDF())

#output

    VOL  count_ask
0    100        163
1    200        141
2    300         96
3    400         89
4    500         84
...

```


#### 3.3 Context by

CONTEXT BY is similar to GROUP BY in terms of grouping. However, with GROUP BY, each group returns a scalar value; with CONTEXT BY, each group returns a vector of the same size as the group's records. In other words, GROUP BY returns the same number of records as the number of unique groups, while CONTEXT BY always returns the same number of records as the total number of records in a table. CONTEXT BY adds great flexibility in data manipulation because it can be used to apply a customized function for each group member.

```

df= s.table(data="trade", dbPath="dfs://valdb").select(["vol","prc"]).where("vol>500").contextby('sym').sum().top(10).toDF()
print(df)

# output
         date   sym    vol     prc
0  2009.08.04  EGAS  11337  8.6000
1  2009.08.05  EGAS   1201  8.5780
2  2009.08.06  EGAS   2463  8.7500
3  2009.08.07  EGAS  13034  8.5800
4  2009.08.10  EGAS  13891  8.2600
5  2009.08.11  EGAS   6276  8.2600
6  2009.08.12  EGAS   4851  8.3900
7  2009.08.13  EGAS  12695  8.5768
8  2009.08.14  EGAS   1612  8.3900
9  2009.08.17  EGAS   1111  8.5000

```


#### 3.4 Regression Analysis

Function ols can be used for regression analysis.Result returns a dictionary containing ANOVA, RegressionStat and Coefficient.

```
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
z=trade.select(['bid','ask','prc']).ols('PRC', ['BID', 'ASK'])

print(z["ANOVA])

# output
    Breakdown    DF            SS            MS             F  Significance
0  Regression     2  29096.227032  14548.113516  381659.15048           0.0
1    Residual  5443    207.476702      0.038118           NaN           NaN
2       Total  5445  29303.703734           NaN           NaN           NaN

print(z["RegressionStat"])

# output

            item   statistics
0            R2     0.992920
1    AdjustedR2     0.992917
2      StdError     0.195239
3  Observations  5446.000000


print(z["Coefficient"])

# output

      factor      beta  stdError       tstat    pvalue
0  intercept -0.035997  0.011709   -3.074184  0.002121
1        BID  1.015587  0.001178  861.871045  0.000000
2        ASK  0.000914  0.000197    4.644738  0.000003

```


#### 4 添加,修改，和删除数据表

#### 4.1 Append table

Data can be appended to a table through function append.

```
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
c1 = trade.count()
print (c1)

# take the top 10 results from the table "trade" and assign it to variable "top10" on the server end
top10 = trade.top(10).executeAs("top10")

# append table "top10" to table "trade"
c2 = trade.append(top10).count()
print (c2)
```


#### 4.2 Update table

Note that function update must be followed by function execute in order to update the table

```
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
trade = trade.update(["VOL"],["200"]).where(["date>1992.01.14"]).execute().where("VOL=200")
t1=trade.where("VOL=200")

print(t1.count())

# output
5437

```

#### 4.2 Delete data from table

Note that function delete must be followed by function execute in order to update the table

```
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
t1=trade.where('date<1992.01.15').delete().execute()
```

#### 4.3 Drop table column(s)

```
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
t1=trade.drop(['ask', 'prc'])
```

#### 4.4 Drop a database table


```
dropTable(WORK_DIR + "/valdb", "trade")
```




  6 update
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
  s.loadText(tableName="USPricesSmall")
 trade = trade.update(["SHRCD", "VOL"],["1", "200"]).where(["date>1992.01.14"])
 print trade.showSQL()
 trade.execute()
 print trade.toDF()
 
  7 top
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 print trade.top(10).toDF()

 8 executeAs: assign a subset to a new table
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 top10 = trade.top(10).executeAs("top10")
 print top10.toDF()

  9 count num of records
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 print trade.count()


  10 append
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 c1 = trade.count
 print c1
 top10 = trade.top(10).executeAs("top10")
 c2 = trade.append(top10).count
 print c2
 assert c1 + 10 == c2


  11 delete
 trade = s.table(dbPath=WORK_DIR+"/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 trade.delete().where('date<1992.01.15').execute()
 top10 = trade.top(10).executeAs("top10")
 print top10.toDF().min()

  12 drop drop column
 trade = s.table(dbPath=WORK_DIR + "/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 trade.drop(['SHRCD', 'HEXCD'])
 print trade.top(10).toDF()

 13 group by
 trade = s.table(dbPath=WORK_DIR + "/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 print trade.select('CUSIP').groupby(['VOL']).count().sort(bys=['VOL desc']).toDF()

 14 group by having
 trade = s.table(dbPath=WORK_DIR + "/PY_USPRICES_SMALL", data="USPricesSmall", inMem=True)
 print trade.select('count(CUSIP)').groupby(['VOL']).having('count(CUSIP)>100').toDF()


15 ols
 trade = s.loadText("USPricesFirst", WORK_DIR + "/data/USPrices_FIRST.csv", parallel=True)
 print trade.select(['bid','ask','prc']).where('bid!=NULL').where('ask!=NULL').ols('PRC', ['BID', 'ASK'])

16 columns, count, schema
 trade = s.loadText("USPricesFirst", WORK_DIR + "/data/USPrices_FIRST.csv", parallel=True)
 print trade.columns
 print trade.count
 print trade.schema

17

 US = loadTable("dfs://US", "US")
 s.close()
 s.connect('38.124.1.173', 8921)
 usprice = s.table(dbPath="dfs://US", data="US")


18
 trade = s.loadText("USPricesFirst", WORK_DIR + "/data/USPrices_FIRST.csv", parallel=True)
 v1=trade.selectAsVector(['VOL', 'PRC'])
 print type(v1)



 print usprice.select('select VOL\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV').where('VOL>0').ols('"turnover"', ['absRet','logMV', 'spread'])


15 ols
US = loadTable("dfs://US", "US")
 s.connect('38.124.1.173', 8921)
 usprice = s.table(dbPath="dfs://US", data="US")

 print usprice.select('select avg(VOL\SHROUT) as turnover, avg(abs(RET)) as absRet, avg((ASK-BID)/(BID+ASK)*2) as spread, avg(log(SHROUT*(BID+ASK)/2)) as logMV').where('VOL>0').groupby('date').ols('"turnover"', ['absRet','logMV', 'spread'])

 DFS
t1 = s.table(data="t1", dbPath="dfs://test1")
print t1.toDF()
  1
quotes = loadTable("dfs://TAQ", "quotes")
 t1 = s.table(data="quotes", dbPath="dfs://TAQ")

  2
  select count(*) from quotes
 df = t1.select("count(*)").toDF()
 print df

  3
  x=select symbol, time, bid, ofr from quotes where symbol='LEH', date=2007.08.21, time=15:59:00
 df = t1.select(["symbol", "time", "bid", "ofr"]).where("symbol='LEH'").where("date=2007.08.21").where("time=15:59:00").toDF()

  4
  avgSpread = select avg((ofr-bid)/(ofr+bid)*2) as avgSpread from quotes where date=2007.08.31, time between 09:30:00 : 15:59:59, ofr>bid, ofr>0, bid>0, ofr/bid<1.2 group by minute(time) as minute
 df = t1.select("avg((ofr-bid)/(ofr+bid)*2) as avgSpread").where("date=2007.08.31, time between 09:30:00 : 15:59:59, ofr>bid, ofr>0, bid>0, ofr/bid<1.2").groupby("minute(time)").toDF()

 5
  select avg((ofr-bid)/(ofr+bid)*2) as avgSpread from quotes where date=2007.09.25, symbol=`LEH, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2 group by minute(time) as minute
 df = t1.select("avg((ofr-bid)/(ofr+bid)*2) as avgSpread").where("date=2007.09.25, symbol='LEH', time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2").groupby("minute(time)").toDF()

  6
 select last((ofr+bid)/2) as last from quotes where date<2007.09.01, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2 group by symbol, date, minute(time) as minute
 df = t1.select("last((ofr+bid)/2) as last").where("date<2007.08.03, time between 09:30:00 : 15:59:59, ofr>bid, bid>0, ofr/bid<1.2").groupby(["symbol", "date", "minute(time)"]).toDF()

  7
 df= s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time","Symbol","Trade_Volume","Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').top(10).toDF()
 print df

  8
 df= s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time","Symbol","Trade_Volume","Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').agg({'trade_volume':['mavg'] }).toDF()


 df= s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time","Symbol","Trade_Volume","Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').having('count(symbol)>3').top(10).toDF()
 print df


s.database('db',partitionType=RANGE, partitions=[10000,20000,30000,40000,50000,60000,70000,80000,90000,100000], dbPath=WORK_DIR+"/USsample")
 t1=s.loadTextEx(dbPath=WORK_DIR+"/USsample", partitionColumns=["PERMNO"], tableName='USsample', filePath=WORK_DIR + "/data/USsample.csv")
 print t1.count

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True, inMemPartitions=["PERMNO"])
 print trade.showSQL()


 t1 = s.loadTableBySQL(dbPath=WORK_DIR+"/USsample", tableName="USsample", sql="select distinct TICKER from USsample")
 print t1.toDF()

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True)
 trade.update(["SHRCD", "VOL"],["SHRCD+1", "200"]).where(["TICKER='AMZN'"]).execute()
 print trade.top(10).toDF()

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True)
 print trade.count

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True)
 top10 = trade.top(10).executeAs("top10")
 print trade.append(top10).count

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True)
 trade.update(["SHRCD", "VOL"],["SHRCD+1", "200"]).where(["TICKER='AMZN'"]).execute()
  print trade.showSQL()

  print trade.toDF()
 print trade.where(["TICKER='AMZN'"]).toDF()

 s = session()
 s.connect('38.124.1.173', 8921)
 US = s.table(dbPath="dfs://US", data="US")
 print US.select('max(PRC)').where('VOL>0').groupby(['date','TICKER']).toDF()

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", partitions=[20000,40000,60000])
 print trade.toDF()


 print US.select('max(PRC)').where(['date>2016.11.30','TICKER>"Y"']).groupby(['date','TICKER']).toDF()

 trade = s.table(dbPath=WORK_DIR+"/USsample", data="USsample", inMem=True)
 trade.update(["SHRCD", "VOL"],["SHRCD+1", "200"]).where(["TICKER='AMZN'"]).execute()

 trade = s.table(dbPath=WORK_DIR + "/USsample", data="USsample", inMem=True)
 trade.drop(['TICKER', 'VOL'])
 print trade.toDF()


 trade = s.table(dbPath=WORK_DIR + "/USsample", data="USsample", inMem=True)
 trade.drop(['TICKER', 'VOL'])
 print trade.top(10).toDF()


 s = session()
 s.connect('38.124.1.173', 8921)
 s.run('dateValue=2007.08.01')
 s.run('num=500')
 quotes = s.table(data='quotes', dbPath="dfs://TAQ")
 quotes.select("count(*)").where(['date=dateValue','time between 09:30:00 : 15:59:59', 'bid>0','bid<ofr','ofr<bid*12.']).groupby('symbol').sort('count desc').executeAs('symCount')
 symCount = s.table(data="symCount")
 syms = symCount.selectAsVector('symbol', vectorAlias="syms")
 quotes.where("date = dateValue, Symbol in syms, 0<bid, bid<ofr, ofr<bid*1.2, time between 09:30:00 : 15:59:59 ").pivotby("time.minute() as minute", "symbol", "avg(bid + ofr)/2.0 as price").selectAsVector(vectorAlias="priceMatrix")
 s.run("retMatrix=each(def(x):ratios(x)-1, priceMatrix)")
 s.run("corrMatrix=cross(corr, retMatrix, retMatrix)")
 s.close()

df = s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time", "Symbol", "Trade_Volume", "Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').top(10).toDF()
 print s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time", "Symbol", "Trade_Volume", "Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').agg({'trade_volume':[ddb.cumsum] }).toDF()
 print s.table(data="trade", dbPath="/data/ssd/DolphinDBDemo/EQY").select(["Time","Symbol","Trade_Volume","Trade_Price"]).where("Trade_Volume>1000000").contextby('symbol').agg2('wavg', ('Trade_Price', 'Trade_Volume')).toDF()
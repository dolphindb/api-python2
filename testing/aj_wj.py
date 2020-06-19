import dolphindb as ddb

s=ddb.session()
s.connect("38.124.1.173",8921, "admin", "123456")

trade = s.loadTable(tableName="trade", dbPath="dfs://EQY")
nbbo = s.loadTable(tableName="nbbo", dbPath="dfs://EQY")

# as of join

trade.merge_asof(nbbo, left_on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost").groupby("symbol").executeAs("TC1")
print(s.table(data="TC1").where("symbol in `GS`TSLA`AAPL").toDF())

trade.merge_window(nbbo,leftBound=-100000000, rightBound=0,aggFunctions="[avg(Offer_Price) as Offer_Price, avg(Bid_Price) as Bid_Price]",left_on=["Symbol","Time"], prevailing=True).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2)/sum(Trade_Volume*Trade_Price))*10000 as cost").groupby("symbol").executeAs("TC2")
print(s.loadTable(tableName="TC2").where("symbol in `GS`TSLA`AAPL").toDF())

print(s.table(data="TC1").merge(s.table(data="TC2"), left_on=["symbol"]).toDF())
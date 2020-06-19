import dolphindb as ddb
import time
s=ddb.session()
s.connect("38.124.1.173",8921, "admin", "123456")
s.clearAllCache(dfs=True)
s.undefAll()
# start_time = time.time()
# quotes = s.loadTable(dbPath="dfs://TAQ", tableName="quotes")
# result = quotes.select("ofr/bid-1 as spread, (bidsiz+ofrsiz)*(ofr+bid)\\2 as quoteSize").where("date between 2007.08.01 : 2007.08.05,time between 09:30:00 : 15:59:59, ofr>bid, ofr>0, bid>0, ofr/bid<1.2").ols("spread", "quoteSize", True)
# print(result)
# print("--- %s seconds ---" % (time.time() - start_time))

US = s.loadTable("US","dfs://US")
result = US.select("select VOL\\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV").where("VOL>0").ols("turnover", ["absRet","logMV", "spread"], True)
print(result["ANOVA"])



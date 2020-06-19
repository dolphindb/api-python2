import dolphindb as ddb

# s=ddb.session()
# s.connect("localhost", 8080, "admin", "123456")

for x in range(2):
    s = ddb.session()
    s.connect("38.124.1.174", 8924, "admin", "123456")
    print(x)
    # s.close()
for x in range(3):
    t1=s.loadTable(dbPath="dfs://EQY", tableName="trade")
    print(t1.rows)

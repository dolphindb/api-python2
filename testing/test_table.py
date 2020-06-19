import unittest
import dolphindb as ddb
rs = lambda s: s.replace(' ', '')

HOST = "38.124.1.173"
PORT = 8921
WORK_DIR = "C/tutorials_en/data"
s = ddb.session(HOST, PORT,"admin","123456") # type: ddb.session


class TestTable(unittest.TestCase):

    def test_basic(self):
        print("test_basic:")
        dt = s.table(data={'vol': [1, 2, 2, 3],
                            'symb': ['APPL', 'A', 'AMNZ', 'A'],
                                  'price': [22, 3.5, 21, 26]})  # type: dolphindb.Table
        print(dt['price'][(dt.price > 10) | (dt.vol < 5.5)].groupby('price,int(vol)').sort(['int(price) desc','vol']).showSQL())
        print(dt.showSQL())
        print(dt.toDF())
        tableName = dt.tableName()

        self.assertEqual(rs(dt.select("price").showSQL()), rs("select price from %s" % tableName))
        a = dt.select("price").toDF()
        b = s.run("select price from %s" % tableName)
        self.assertEqual(a.equals(b), True)

        self.assertEqual(rs(dt.showSQL()), rs("select price,symb,vol from %s " % tableName))
        self.assertEqual(dt.toDF().equals(s.run("select price,symb,vol from %s " % tableName)), True)

        self.assertEqual(rs(dt.groupby('symb').showSQL()), rs("select price,symb,vol from %s group by symb" % tableName))

        self.assertEqual(rs(dt['price', 'vol'].showSQL()), rs("select price,vol from %s " %tableName))
        self.assertEqual(dt['price', 'vol'].toDF().equals(s.run("select price,vol from %s " %tableName)), True)

        self.assertEqual(rs(dt['price > 10'].showSQL()), rs("select price>10 from %s " % tableName))
        self.assertEqual(dt['price > 10'].toDF().equals(s.run("select price>10 from %s " % tableName)), True)

        self.assertEqual(rs(dt[dt.price > 10].showSQL()), rs("select price,symb,vol from %s where (price > 10)" % tableName))
        self.assertEqual(dt[dt.price > 10].toDF().equals(s.run("select price,symb,vol from %s where (price > 10)" % tableName)), True)
        #
        self.assertEqual(rs(dt[(dt.price > 10) | (dt.vol < 5.5)].showSQL()), rs("select price,symb,vol from %s where ((price > 10) or (vol < 5.5))" % tableName))
        self.assertEqual(dt[(dt.price > 10) | (dt.vol < 5.5)].toDF().equals(s.run("select price,symb,vol from %s where ((price > 10) or (vol < 5.5))" % tableName)),True)

        self.assertEqual(rs(dt[(dt.price > 10) | (dt.vol < 5.5)].groupby('price,symb,vol').showSQL()), rs("select price,symb,vol from %s where ((price > 10) or (vol < 5.5)) group by price,symb,vol" % tableName))

        self.assertEqual(rs(dt[(dt.price > 10) | (dt.vol < 5.5)].groupby('price,int(vol)').showSQL()),rs("select price,symb,vol from %s where ((price > 10) or (vol < 5.5)) group by price,int(vol)" % tableName))

        self.assertEqual(rs(dt.where(dt.price > 10).showSQL()), rs("select price,symb,vol from %s where (price > 10)" % tableName))
        self.assertEqual(dt.where(dt.price > 10).toDF().equals(s.run("select price,symb,vol from %s where (price > 10)" % tableName)), True)


        self.assertEqual(rs(dt.sort('vol').showSQL()), rs("select price,symb,vol from %s order by vol" % tableName))
        self.assertEqual(dt.sort('vol').toDF().equals( s.run("select price,symb,vol from %s order by vol" % tableName)), True)

        self.assertEqual(rs(dt.sort('vol asc').showSQL()), rs("select price,symb,vol from %s order by vol asc" % tableName))
        self.assertEqual(dt.sort('vol asc').toDF().equals(s.run("select price,symb,vol from %s order by vol asc" % tableName)),True)

        self.assertEqual(rs(dt.sort(['vol', 'int(price) desc']).showSQL()), rs("selectprice,symb,vol from %s order by vol,int(price) desc" % tableName))
        self.assertEqual(
            dt.sort(['vol', 'int(price) desc']).toDF().equals(s.run("select price,symb,vol from %s order by vol,int(price) desc" % tableName)), True)

        self.assertEqual(rs(dt['price'][(dt.price > 10) | (dt.vol < 5.5)].groupby('price,int(vol)').sort(['vol', 'int(price) desc']).showSQL()),
                         rs("select price from %s where ((price > 10) or (vol < 5.5)) group by price, int(vol) order by vol,int(price) desc" % tableName))

        self.assertEqual(rs(dt['price'].groupby('symb').showSQL()), rs('select price from %s group by symb' % tableName))

        self.assertEqual(rs(dt.groupby('symb').sum().showSQL()), rs('select sum(price),sum(vol) from %s group by symb' % tableName))
        self.assertEqual(dt.groupby('symb').sum().toDF().equals(s.run('select sum(price),sum(vol) from %s group by symb' % tableName)), True)

        self.assertEqual(rs(dt.groupby('symb').agg(['sum']).showSQL()),
                         rs('select sum(price),sum(vol) from %s group by symb' % tableName))
        self.assertEqual(dt.groupby('symb').agg(['sum']).toDF().equals(s.run('select sum(price),sum(vol) from %s group by symb ' % tableName)),True)


        self.assertEqual(rs(dt.groupby('symb').wsum(('price', 'vol')).showSQL()),
                         rs('select wsum(price,vol) from %s group by symb' % tableName))
        self.assertEqual(dt.groupby('symb').wsum(('price', 'vol')).toDF().equals(s.run('select wsum(price,vol) from %s group by symb ' % tableName)), True)

        self.assertEqual(rs(dt.groupby('symb').wsum([('price', 'vol'), ('vol', 'price')]).showSQL()),
                         rs('select wsum(price,vol),wsum(vol,price) from %s group by symb' % tableName))
        self.assertEqual(dt.groupby('symb').wsum([('price', 'vol'), ('vol', 'price')]).toDF().equals(s.run('select wsum(price,vol),wsum(vol,price) from %s group by symb' % tableName)), True)

        self.assertEqual(rs(dt.groupby('symb').agg({'price':[ddb.sum]}).showSQL()),rs('select sum(price)from %s group by symb'%tableName))
        self.assertEqual(dt.groupby('symb').agg({'price': [ddb.sum]}).toDF().equals(s.run('select sum(price)from %s group by symb' % tableName)),True)

        #self.assertEqual(dt.groupby('symb').agg({'price': [ddb.sum, ddb.avg]}).toDF().equals(s.run('select sum(price),avg(price) from %s group by symb' % tableName)), True)
        self.assertEqual(rs(dt.pivotby('symb', 'vol', 'price').showSQL()), rs('select price from %s pivot by symb, vol' % tableName))
        self.assertEqual(dt.pivotby('symb', 'vol', 'price').toDF().equals(
                         s.run('select price from %s pivot by symb, vol' % tableName)), True)

        self.assertEqual(rs(dt.pivotby('symb', 'vol', 'price', ddb.sum).showSQL()),
                         rs('select sum(price) from %s pivot by symb, vol' % tableName))
        self.assertEqual(dt.pivotby('symb', 'vol', 'price', ddb.sum).toDF().equals(
                         s.run('select sum(price) from %s pivot by symb, vol' % tableName)),True)

        print(dt.pivotby('vol', 'symb',  'price').toDF())

        print(dt.contextby('symb').agg({'price':[ddb.sum]}).showSQL())

        print('select symb, sum(price) from %s context by symb' % tableName)

        self.assertEqual(rs(dt.contextby('symb').agg({'price':[ddb.sum]}).showSQL()),
                         rs('select symb, sum(price) from %s context by symb' % tableName))
        print(dt.contextby('symb').agg({'price':[ddb.sum]}).showSQL())

    def test_merge(self):
        print("\ntest_merge:")
        dt1 = s.table(data={'id': ['APPL', 'A', 'AMNZ', 'A'],
                        'vol': [1, 2, 2, 3],
                        'price': [22, 3.5, 21, 26]})  # type: dolphindb.Table
        print(s.run(dt1.tableName()))
        dt2 = s.table(data={'id': ['APPL', 'A', 'MSFT', 'A'],
                        'vol': [1, 2, 3, 5],
                        'price': [23, 3.5, 20, 11]})  # type: dolphindb.Table
        print(s.run(dt2.tableName()))
        n1 = dt1.tableName()
        n2 = dt2.tableName()
        # if n1 > n2:
        #     # select = " %s.vol as rhs_vol, %s.price as rhs_price, %s.id as rhs_id,%s.vol as lhs_vol, %s.price as lhs_price, %s.id as lhs_id" % (n2, n2, n2, n1, n1, n1)
        #     select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n1, n1, n1, n1, n1, n1)
        # else:
        #     # select = " %s.vol as lhs_vol, %s.price as lhs_price, %s.id as lhs_id,  %s.vol as rhs_vol, %s.price as rhs_price, %s.id as rhs_id" % (n1, n1, n1, n2, n2, n2)
        #     select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n2, n2, n2, n2, n2, n2)

        select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n2, n2, n2, n2, n2, n2)
        print(dt1.merge(dt2, left_on='id', right_on='id').showSQL())
        print("select * from ej(%s,%s,`id, `id)" % ( n1, n2))
        print("*********************************")
        print(dt1.merge(dt2, left_on='id', right_on='id').showSQL())
        self.assertEqual(rs(dt1.merge(dt2, left_on='id', right_on='id').showSQL()), \
                         rs("select * from ej(%s,%s,`id, `id)" \
                            % (n1, n2)))
        print(dt1.merge(dt2, left_on='id', right_on='id').toDF())

        select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n2, n2, n2, n2, n2, n2)
        self.assertEqual(rs(dt1.merge(dt2, how='left', left_on='id', right_on='id').showSQL()), \
                         rs("select * from lj(%s,%s,`id, `id)" % (n1, n2)))
        print(dt1.merge(dt2, how='left', left_on='id', right_on='id').toDF())

        select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n1, n1, n1, n1, n1, n1)
        self.assertEqual(rs(dt1.merge(dt2, how='right', left_on='id', right_on='id').showSQL()), \
                         rs("select * from lj(%s,%s,`id, `id)" % ( n2, n1)))
        print(dt1.merge(dt2, how='right', left_on='id', right_on='id').toDF())

        select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n2, n2, n2, n2, n2, n2)
        self.assertEqual(rs(dt1.merge(dt2, how='outer', left_on='id', right_on='id').showSQL()), \
                         rs("select * from fj(%s,%s,`id, `id)" % ( n1, n2)))
        self.assertEqual(rs(dt1.merge(dt2, how='outer', left_on='id').showSQL()), \
                         rs("select * from fj(%s,%s,`id, `id)" % (n1, n2)))
        self.assertEqual(rs(dt1.merge(dt2, how='outer', right_on='id').showSQL()), \
                         rs("select * from fj(%s,%s,`id, `id)" % (n1, n2)))
        self.assertEqual(rs(dt1.merge(dt2, how='outer', on='id').showSQL()), \
                         rs("select * from fj(%s,%s,`id, `id)" % (n1, n2)))
        print(dt1.merge(dt2, how='outer', left_on='id', right_on='id').toDF())

        self.assertEqual(rs(dt1.merge(dt2, how='outer', left_on='id', right_on='id').showSQL()), \
                         rs("select * from fj(%s,%s,`id, `id)" % (n1, n2)))
        print(dt1.merge(dt2, how='outer', left_on='id', right_on='id').toDF())

    def test_merge_asof(self):
        print("\ntest_merge_asof:")
        dt1 = s.table(data={'id': ['APPL', 'A', 'AMNZ', 'A'],
                              'vol': [1, 2, 3, 5],
                              'price': [22, 3.5, 21, 26]})  # type: dolphindb.Table
        print(s.run(dt1.tableName()))
        dt2 = s.table(data={'id': ['APPL', 'A', 'MSFT', 'A'],
                              'vol': [1, 2, 2, 3],
                              'price': [23, 3.5, 20, 11]})  # type: dolphindb.Table
        n1 = dt1.tableName()
        n2 = dt2.tableName()
        # if n1 > n2:
        #     select = "%s.id as rhs_id, %s.price as rhs_price, %s.vol as rhs_vol, %s.id as lhs_id, %s.price as lhs_price, %s.vol as lhs_vol" % (
        #     n2, n2, n2, n1, n1, n1)
        # else:
        #     select = "%s.id as lhs_id, %s.price as lhs_price, %s.vol as lhs_vol, %s.id as rhs_id, %s.price as rhs_price, %s.vol as rhs_vol" % (
        #     n1, n1, n1, n2, n2, n2)
        #
        select = " id,vol,price,%s.id as %s_id,%s.vol as %s_vol,%s.price as %s_price" % (n2, n2, n2, n2, n2, n2)

        print(dt1.merge_asof(dt2, on='vol').showSQL())
        print("select * from aj(%s,%s,`vol, `vol)" % (n1, n2))
        self.assertEqual(rs(dt1.merge_asof(dt2, on='vol').showSQL()),
                         rs("select * from aj(%s,%s,`vol, `vol)" % (n1, n2)))
        print(dt1.merge_asof(dt2, on='vol').toDF())

    def test_merge_window(self):
        if s.existsDatabase(WORK_DIR+"/windowJoin2"):
            s.dropDatabase(WORK_DIR+"/windowJoin2")
        s.database(dbName='db', dbPath=WORK_DIR+"/windowJoin2")
        s.run("t1 = table(`A`A`B as sym, 09:56:06 09:56:07 09:56:06 as time, 10.6 10.7 20.6 as price)")
        s.run("saveTable(db, t1, `t1)")
        s.run("t2 = table(take(`A,10) join take(`B,10) as sym, take(09:56:00+1..10,20) as time, (10+(1..10)\ 10-0.05) join (20+(1..10)\ 10-0.05) as bid, (10+(1..10)\ 10+0.05) join (20+(1..10)\ 10+0.05) as offer, take(100 300 800 200 600, 20) as volume);")
        s.run("saveTable(db, t2, `t2)")
        s.run("t3=t2")
        s.run("t3.rename!(`time, `second)")
        s.run("saveTable(db, t3);")
        s.run("t4=t2")
        s.run("delete from t4 where 09:56:04<=time<=09:56:06")
        s.run("saveTable(db, t4);")

        dt1 = s.table(dbPath=WORK_DIR + "/windowJoin2", data="t1", inMem=True)
        df1 = dt1.toDF()
        print(df1)
        dt2 = s.table(dbPath=WORK_DIR + "/windowJoin2", data="t2", inMem=True)
        df2 = dt2.toDF()
        print(df2)
        dt3 = s.table(dbPath=WORK_DIR + "/windowJoin2", data="t3", inMem=True)
        df3 = dt3.toDF()
        print(df3)
        dt4 = s.table(dbPath=WORK_DIR + "/windowJoin2", data="t4", inMem=True)
        df4 = dt4.toDF()
        print(df4)

        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='avg(bid)', leftBound=-5, rightBound=0, ).showSQL())
        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='avg(bid)', leftBound=-5, rightBound=0, ).toDF())

        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='[wavg(bid,volume), wavg(offer,volume)]', leftBound=-5,
                               rightBound=-1, ).showSQL())
        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='[wavg(bid,volume), wavg(offer,volume)]', leftBound=-5,
                               rightBound=-1, ).toDF())

        print(dt1.merge_window(dt3, left_on='sym`time', right_on="sym`second", aggFunctions='[wavg(bid,volume), wavg(offer,volume)]', leftBound=-2,
                               rightBound=2, ).showSQL())
        print(dt1.merge_window(dt3, left_on='sym`time', right_on="sym`second", aggFunctions='[wavg(bid,volume), wavg(offer,volume)]', leftBound=-2,
                               rightBound=2, ).toDF())

        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='[last(bid) as bid, last(offer) as offer]', leftBound=-100, rightBound=0, ).showSQL())
        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='[last(bid) as bid, last(offer) as offer]', leftBound=-100, rightBound=0, ).toDF())

        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='avg(bid)', leftBound=-5, rightBound=0, ).showSQL())
        print(dt1.merge_window(dt2, on='sym`time', aggFunctions='avg(bid)', leftBound=-5, rightBound=0, ).toDF())

        print(dt1.merge_window(dt4, on='sym`time', aggFunctions='[first(bid), avg(offer)]', leftBound=-1,
                               rightBound=1, ).showSQL())
        print(dt1.merge_window(dt4, on='sym`time', aggFunctions='[first(bid), avg(offer)]', leftBound=-1,
                               rightBound=1, ).toDF())

        print(dt1.merge_window(dt4, on='sym`time', aggFunctions='[first(bid), avg(offer)]', leftBound=-1, rightBound=1, prevailing=True).showSQL())
        print(dt1.merge_window(dt4, on='sym`time', aggFunctions='[first(bid), avg(offer)]', leftBound=-1, rightBound=1, prevailing=True).toDF(), )


    def test_pull_remote_table(self):
        print("\ntest_pull_remote_table:")
        """
        assume the following is executed on remote dolphindb server:

        n = 100000
        share table(rand(`APPL`A`AMNZ`GOOG, n) as symb, int(rand(100,n) + 1) as vol, rand(200, n) as price) as t

        """
        s.run("n = 100000")
        s.run("share table(rand(`APPL`A`AMNZ`GOOG, n) as symb, int(rand(100,n) + 1) as vol, rand(200, n) as price) as t")
        dt = s.table(data="t")
        df = dt.toDF()
        print(df)
        self.assertEqual(len(df.index), s.run("size(t)"))
        df = dt.groupby("symb").agg2('wavg', ('price', 'vol')).toDF()
        print(df)

if __name__ == '__main__':
    unittest.main()
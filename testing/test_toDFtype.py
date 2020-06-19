import unittest
import dolphindb as ddb
import numpy as np
s = ddb.session()
s.connect("localhost",8848,"admin","123456")

s.run("t = table(100000:0,`cid`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp"
      "`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,"
      "SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])")


class TestDFTypes(unittest.TestCase):
    def test_non_null_types(self):
        s.run("insert into t values(0,true,'a',122h,123,22l,2012.06.12,2012.06M,13:10:10.008,13:30m,13:30:10,"
              "2012.06.13 13:30:10,2012.06.13 13:30:10.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,`ABC,`abc)")
        t = s.run("t")
        self.assertEqual(t['cid'].dtype, np.int64)
        self.assertEqual(t['cbool'].dtype, np.bool)
        self.assertEqual(t['cchar'].dtype, np.int64)
        self.assertEqual(t['cshort'].dtype, np.int64)
        self.assertEqual(t['cint'].dtype, np.int64)
        self.assertEqual(t['clong'].dtype, np.int64)
        self.assertEqual(t['cdate'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cmonth'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['ctime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cminute'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['csecond'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cdatetime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['ctimestamp'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cnanotime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cnanotimestamp'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cfloat'].dtype, np.float64)
        self.assertEqual(t['cdouble'].dtype, np.float64)
        self.assertEqual(t['csymbol'].dtype, np.object)
        self.assertEqual(t['cstring'].dtype, np.object)

    def test_null_types(self):
        s.run("insert into t values(1,,,,,,,,,,,,,,,,,'','')")
        t = s.run("t")
        self.assertEqual(t['cid'].dtype, np.int64)
        self.assertEqual(t['cbool'].dtype, np.object)
        self.assertEqual(t['cchar'].dtype, np.float64)
        self.assertEqual(t['cshort'].dtype, np.float64)
        self.assertEqual(t['cint'].dtype, np.float64)
        self.assertEqual(t['clong'].dtype, np.float64)
        self.assertEqual(t['cdate'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cmonth'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['ctime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cminute'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['csecond'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cdatetime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['ctimestamp'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cnanotime'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cnanotimestamp'].dtype, np.datetime64(0, 'ns').dtype)
        self.assertEqual(t['cfloat'].dtype, np.float64)
        self.assertEqual(t['cdouble'].dtype, np.float64)
        self.assertEqual(t['csymbol'].dtype, np.object)
        self.assertEqual(t['cstring'].dtype, np.object)


if __name__ == '__main__':
    unittest.main()


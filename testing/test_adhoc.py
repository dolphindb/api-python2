import dolphindb as ddb
s = ddb.session()
s.connect("localhost",8801,"admin","123456")
import numpy as np
import pandas as pd
from dolphindb.type_util import *
from datetime import datetime
from datetime import timedelta

def test1():
    data={
        'pint':[0,1,intNan],
         'pfloat':[0.5,5.5, floatNan],
         'pbool': [True, False, boolNan],
         'pstring':['hello',None,'world'],
        # 'pdatetime64_1':[datetime(2017,6,27,15,56,56,167000),datetime(2017,6,27,0,0,0,0),np.NAN],
        # 'pdatetime64_2':[datetime(2011,6,12),np.nan,datetime(2012,12,30)],
        # 'pdatetime64_3':[datetime(2012,5,14,3),np.NAN,datetime(2012,5,14,4)],
        # 'pdatetime64_4':[datetime(2016,6,14,3,15),datetime(2016,7,15,16,30),np.NAN],
        #  'pdatetime65_5':[datetime(2016,8,26,6,12,30),datetime(2016,8,27,19,56,24),np.datetime64('NaT') ]
        #    'time1':[Time.from_time(time(5,6,7)),Time.from_time(time(5,6,7)),Time.from_time(time(5,6,7))]
         }

    from pandas import Series,DataFrame
    df=DataFrame(data)
    print (df.dtypes)
    dd=s.table(data=df).executeAs("t1")
    # print(s.run("exec pbool from t1"))
    df2=dd.toDF()
    print(df2)
    print(df2.dtypes)


def test2():
    test2 = s.loadTable(dbPath="C:/DolphinDB/db_testing/testPython/", tableName="t1")
    df = test2.toDF()
    print(df)
    print(df["ctime"])
    print(df["cminute"])
# s.upload({"a":np.bool_([True, False,None,True])})
# print(s.run("a"))

# test2()

def test3():
    test3 = s.loadTable(dbPath="dfs://testPython", tableName="t4")
    data5 = {'p0': [1, 2, 3, 3],
             'p1': [5,6, intNan, 201],
             'p2': [0.5, np.NaN, 100.24, 20.24]}
    df5 = pd.DataFrame(data5)
    print(df5)
    test3.append(s.table(data=data5))
# s.upload({'a':[True, False, boolNan]})
# print(s.run('a'))
# print(s.run('typestr(a)'))
# df = pd.DataFrame({'a':[True, boolNan, False]})
# print(df)
# print(df.dtypes)
# s.upload({'b':pd.DataFrame({'a':[True, -128, False]})})
# print(s.run("b"))
test1()


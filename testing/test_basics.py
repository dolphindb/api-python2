import numpy as np
import sys
import datetime
sys.path.append('..')
from dolphindb import *

if __name__ == '__main__':

    conn = session()
    success = conn.connect('172.16.95.128', 8921,"admin","123456")
    id = 1;
    WORK_DIR = "C:/DolphinDB/data"
    if success:
        print("hello")

        print("test_window:")

        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing double Vector")
        # timeStart = datetime.now()
        # vc = conn.run('rand(1000.0,10000)')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds/1000))
        # print(len(vc), vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing String Vector")
        # timeStart = datetime.now()
        # vc = conn.run('rand(`IBM`MSFT`GOOG`BIDU,10000)')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(len(vc), vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing Dictionary")
        # timeStart = datetime.now()
        # vc = conn.run('dict(1 2 3, `IBM`MSFT`GOOG)')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print ("Testing matrix")
        # timeStart = datetime.now()
        # matrix, rowlabels, colLables = conn.run('1..6$3:2')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(matrix)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing table")
        # timeStart = datetime.now()
        # table_str = "n=20000\n"
        # table_str += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"
        # table_str += "t1=table(2016.08.09 09:30:00.000+rand(18000,n) as timestamp,rand(syms,n) as sym,100*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);\n"
        # table_str += "select sym,qty,price from t1 where price>9"
        # df = conn.run(table_str)
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(df)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function add integer")
        # timeStart = datetime.now()
        # vc = conn.run('add',1334,-334)
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sub float")
        # timeStart = datetime.now()
        # vc = conn.run('sub', 97.62, -32.38)
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function add string")
        # timeStart = datetime.now()
        # vc = conn.run('add', 'add', 'string')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sum  list float")
        # timeStart = datetime.now()
        # vc = conn.run('sum', [1.0, 2.0, 3.0])
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function dict keys")
        # timeStart = datetime.now()
        # vc = conn.run('keys', {"ibm":100.0, "ms":120.0, "c": 130.0})
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function dict values")
        # timeStart = datetime.now()
        # vc = conn.run('values', {"ibm":100.0, "ms":120.0, "c": 130.0})
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sum  numpy array int32 ")
        # timeStart = datetime.now()
        # vc = conn.run("sum", np.array([100000, 200000, 300000]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sum  numpy array int64 ")
        # timeStart = datetime.now()
        # vc = conn.run("sum", np.int64([1e15, 2e15, 3e15]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sum  numpy array float64 ")
        # timeStart = datetime.now()
        # vc = conn.run("sum", np.array([100000.0, 200000.0, 300000.0]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function sum  numpy array bool ")
        # timeStart = datetime.now()
        # vc = conn.run("sum", np.bool_([True, False, True, False]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function reverse str vector")
        # timeStart = datetime.now()
        # vc = conn.run("reverse", np.array(["1", "2", "3"],dtype="str"))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function user defined function")
        # timeStart = datetime.now()
        # conn.run("def f(a,b) {return a+b};")
        # vc = conn.run("f", 1, 2.0)
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function flatten int matrix")
        # timeStart = datetime.now()
        # vc = conn.run("flatten", np.int32([[1, 2, 3], [4, 5, 6]]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing function flatten double matrix")
        # timeStart = datetime.now()
        # vc = conn.run("flatten", np.double([[1, 2, 3], [4.0, 5.0, 6.0]]))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print ("Testing matrix upload")
        # timeStart = datetime.now()
        # (a, _, _) = conn.run("cross(+, 1..5, 1..5)")
        # (b, _, _) = conn.run("1..25$5:5")
        # nameObjectDict = {'a':a, 'b':b}
        # conn.upload(nameObjectDict)
        # (vc, _, _) =conn.run("a+b")
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test set read")
        # timeStart =datetime.now()
        # vc = conn.run('set([5, 5, 3, 4])')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test set upload")
        # timeStart = datetime.now()
        # x = {5, 5, 4, 3}
        # y = {8, 9, 9, 4, 6}
        # nameObjectDict = {'x': x, 'y': y}
        # conn.upload(nameObjectDict)
        # vc = conn.run('x | y')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test pair read")
        # timeStart = datetime.now()
        # vc = conn.run('3:4')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        #
        # print("Testing function cast double matrix")
        # timeStart = datetime.now()
        # x = np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        # vc = conn.run("cast", np.double([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]), Pair(2,3))
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc[0])
        #
        # print("---------------------------------------------------",id)
        # id += 1
        # print("Test any vector")
        # timeStart = datetime.now()
        # x = [1, 2, "a", 'b']
        # conn.upload({'x': x})
        # vc = conn.run('x[1:3]')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing Date scalar")
        # timeStart = datetime.now()
        # vc = conn.run('2012.10.01')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("Testing Date scalar")
        # timeStart = datetime.now()
        # vc = conn.run('1904.02.29')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("Testing Date scalar")
        # timeStart = datetime.now()
        # vc = conn.run('1904.01.01 + 365')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test date vector read/upload/read")
        # timeStart = datetime.now()
        # dates = conn.run('2012.10.01 + rand(1000,1000)')
        # tmp = dates
        # conn.upload({'dates': np.array(list(dates))})
        # vc = conn.run('dates')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(list(dates))
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test month vector read/upload/read")
        # timeStart = datetime.now()
        # months = conn.run('2012.01M+rand(11,10)')
        # conn.upload({'months': list(months)})
        # vc = conn.run('months')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(months)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test time vector read/upload/read")
        # timeStart = datetime.now()
        # times = conn.run('12:32:56.356 + (rand(1000000,10))')
        # conn.upload({'times': np.array(list(times))})
        # vc = conn.run('times')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(list(times))
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test nanotime vector read/upload/read")
        # timeStart = datetime.now()
        # times = conn.run('12:32:56.356000000 + (rand(1000000,10))')
        # conn.upload({'times': np.array(list(times))})
        # vc = conn.run('times')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(list(times))
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test minute vector read/upload/read")
        # timeStart = datetime.now()
        # minutes = conn.run('12:30m+rand(100,10)')
        # conn.upload({'minutes': np.array(list(minutes))})
        # vc = conn.run('minutes')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(minutes)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test second vector read/upload/read")
        # timeStart = datetime.now()
        # seconds = conn.run('12:56:38+rand(1000,10)')
        # conn.upload({'seconds': np.array(list(seconds))})
        # vc = conn.run('seconds')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(seconds)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test datetime vector read/upload/read")
        # timeStart = datetime.now()
        # datetimes = conn.run('2012.10.01T15:00:04 + rand(10000,10)')
        # conn.upload({'datetimes': np.array(list(datetimes))})
        # vc = conn.run('datetimes')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(datetimes)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test Timestamp scalar read/upload/read")
        # timeStart = datetime.now()
        # timeStamp = conn.run('2012.10.01T15:00:04.008')
        # conn.upload({'timeStamp':timeStamp})
        # vc = conn.run('timeStamp')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(timeStamp)
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test timeStamp vector read/upload/read")
        # timeStart = datetime.now()
        # timeStamps = conn.run('2012.10.01T15:00:04.008 + rand(10000,10)')
        # conn.upload({'timeStamps': np.array(list(timeStamps))})
        # vc = conn.run('timeStamps')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(timeStamps)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test NanoTimestamp scalar read/upload/read")
        # timeStart = datetime.now()
        # nanoTimeStamp = conn.run('2012.10.01T15:00:04.008567123')
        # conn.upload({'nanoTimeStamp': nanoTimeStamp})
        # vc = conn.run('nanoTimeStamp')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(nanoTimeStamp)
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test NanoTimestamp vector read/upload/read")
        # timeStart = datetime.now()
        # timeStamps = conn.run('2012.10.01T15:00:04.856123123 + rand(10000,10)')
        # conn.upload({'timeStamps': np.array(list(timeStamps))})
        # vc = conn.run('timeStamps')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(timeStamps)
        # print(list(vc))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Testing table upload")
        # timeStart = datetime.now()
        # df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]),
        #                     'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]),
        #                     'x': np.int32([5, 4, 3, 2, 1])
        #                     })
        # df2 = pd.DataFrame({'id': np.int32([3, 1]),
        #                     'qty':  np.int32([500, 800]),
        #                     'x': np.double([66.0, 88.0])
        #                     })
        # nameObjectDict = {'t1': df, 't2': df2}
        # conn.upload(nameObjectDict)
        # vc = conn.run("lj(t1, t2, `id)")
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(vc)
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test int nan")
        # timeStart = datetime.now()
        # l = [1, 2, np.nan, 3, 4, np.nan]
        # l2 = map(lambda x:  intNan if pd.isnull(x)  else x, l)
        # conn.upload({'l2':list(l2)})
        # vc = conn.run('l2')
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # print(l, len(list(l)))
        # print(l2, len(list(l2)))
        # print(vc, len(list(vc)))
        #
        # print("---------------------------------------------------", id)
        # id += 1
        # print("Test datetime nan")
        # timeStart = datetime.now()
        # datetimes = conn.run('2012.10.01T15:00:04 + rand(10000,10)')
        # datetimes2 = list(copy.copy(datetimes))
        # datetimes2[0] = Datetime.null()
        # datetimes2[3] = Datetime.null()
        # datetimes2[4] = Datetime.null()
        # datetimes2[9] = Datetime.null()
        # conn.upload({'datetimes2':datetimes2})
        # vc = conn.run('datetimes2')
        # l = map(lambda x: Datetime.isnull(x), vc)
        # print("running time (in millisecond): " + str((datetime.now() - timeStart).microseconds / 1000))
        # # print(list(datetimes), len(list(datetimes)))
        # print(datetimes2, len(datetimes2))
        # # print(list(vc), len(list(vc)))
        # print(list(l))
        #
        # print(conn.run("nanotimestamp(long(nanotimestamp(now())))"))

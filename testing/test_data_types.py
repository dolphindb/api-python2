import unittest
import sys
sys.path.append('..')
from dolphindb import *
# from .settings import HOST, PORT

#setup db connection
HOST = "localhost"
PORT = 8080
xx = session()
xx.connect(HOST, PORT)


class TestDataTypes(unittest.TestCase):
    def test_get_bool(self):
        vc =xx.run('true false true')
        self.assertTrue(np.array_equal(vc, np.array([True,False,True])))

    def test_get_bool(self):
        vc = xx.run('true false true')
        self.assertTrue(np.array_equal(vc, np.array([True, False, True])))


if __name__ == '__main__':
    unittest.main()


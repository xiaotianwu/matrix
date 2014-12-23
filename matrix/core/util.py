__author__ = 'xiaotian.wu'

try:
    from hashlib import md5
except:
    from md5 import md5

def MD5(s):
    m = md5()
    m.update(s)
    return m.hexdigest()

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

class Locking():
    '''scope locking'''
    def __init__(self, locker):
        self._locker = locker

    def __enter__(self):
        self._locker.acquire()

    def __exit__(self, e_t, e_v, e_b):
        self._locker.release()

if __name__ == '__main__':
    import threading
    import unittest

    class MD5Test(unittest.TestCase):
        def testMD5(self):
            print MD5("abcdefg")

    class EnumTest(unittest.TestCase):
        def testSet(self):
            testEnum = Enum(["A", "B"])
            self.assertEqual(testEnum.A, "A")
            self.assertEqual(testEnum.B, "B")
            self.assertEqual(testEnum.A, testEnum.A)
            self.assertEqual(testEnum.B, testEnum.B)

    class LockingTest(unittest.TestCase):
        def do_nothing(self):
            pass

        def testLocker(self):
            with Locking(threading.Lock()):
                self.do_nothing()

    unittest.main()

__author__ = 'xiaotian.wu'

try:
  from hashlib import md5
except:
  from md5 import md5

def MD5(s):
  m = md5()
  m.update(s)
  return m.hexdigest()

def object_to_dict(obj):
  dictionary = dict()
  dictionary['__class__'] = obj.__class__.__name__
  dictionary['__module__'] = obj.__module__
  dictionary.update(obj.__dict__)
  return dictionary

def dict_to_object(dictionary):
  if '__class__' in dictionary:
    class_name = dictionary.pop('__class__')
    module_name = dictionary.pop('__module__')
    _module = __import__(module_name)
    _class = getattr(_module, class_name)
    args = dict((k.encode('ascii'), v) for k, v in dictionary.items())
    print args
    instance = _class(**args)
  else:
    instance = dictionary
  return instance

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

  class TestStruct:
    def __init__(self, a = 1, b = 2):
      self.a = a
      self.b = b

  class ObjectDictTest(unittest.TestCase):
    def testObjectToDict(self):
      t1 = TestStruct()
      t2 = object_to_dict(t1)
      print t2
      print dict_to_object(t2)

  unittest.main()

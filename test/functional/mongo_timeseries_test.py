'''
Functional tests for redis timeseries
'''
import time
import datetime

from pymongo import *
from chai import Chai

from kairos.mongo_timeseries import *

class MongoSeriesTest(Chai):

  def setUp(self):
    super(MongoSeriesTest,self).setUp()

    self.client = MongoClient('localhost')
    self.series = Timeseries(self.client, type='series', prefix='kairos',
      read_func=int, #write_func=str, 
      intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    #self.series.delete('test')

  def test_insert(self):
    self.series.insert( 'test', 32 )

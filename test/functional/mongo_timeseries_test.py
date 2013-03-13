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
    self.series.delete('test')

  def test_insert(self):
    assert_equals( 0, self.series._client['minute'].count() )
    assert_equals( 0, self.series._client['hour'].count() )
    self.series.insert( 'test', 32 )
    assert_equals( 1, self.series._client['minute'].count() )
    assert_equals( 1, self.series._client['hour'].count() )

  def test_delete(self):
    # technically already tested between setup and insert, but here for completeness
    assert_equals( 0, self.series._client['minute'].count() )
    assert_equals( 0, self.series._client['hour'].count() )
    self.series.insert( 'test', 32 )
    assert_equals( 1, self.series._client['minute'].count() )
    assert_equals( 1, self.series._client['hour'].count() )
    self.series.delete('test')
    assert_equals( 0, self.series._client['minute'].count() )
    assert_equals( 0, self.series._client['hour'].count() )

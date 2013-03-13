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

  def test_get(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )
    
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=100 )
    assert_equals( [60], interval.keys() )
    assert_equals( list(range(60,120)), interval[60] )

    # end of an interval
    interval = self.series.get( 'test', 'minute', timestamp=59 )
    assert_equals( [0], interval.keys() )
    assert_equals( list(range(1,60)), interval[0] )
    
    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, len(interval.values()[0]) )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    assert_equals( 60, len(interval) )
    assert_equals( list(range(60,120)), interval[60] )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( list(range(1,3600)), interval[0] )
  
  def test_series(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', timestamp=250 )
    assert_equals( [0,60,120,180,240], interval.keys() )
    assert_equals( list(range(1,60)), interval[0] )
    assert_equals( list(range(240,300)), interval[240] )
    
    interval = self.series.series( 'test', 'minute', steps=2, timestamp=250 )
    assert_equals( [180,240], interval.keys() )
    assert_equals( list(range(240,300)), interval[240] )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', timestamp=250 )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[0]) )
    assert_equals( list(range(1,60)), interval[0][0] )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200 )
    assert_equals( 1, len(interval) )
    assert_equals( 3600, len(interval[3600]) )
    assert_equals( list(range(3600,7200)), interval[3600] )

    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200, steps=2 )
    assert_equals( [0,3600], interval.keys() )
    assert_equals( 3599, len(interval[0]) )
    assert_equals( 3600, len(interval[3600]) )
    assert_equals( list(range(3600,7200)), interval[3600] )

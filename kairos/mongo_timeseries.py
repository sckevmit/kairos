'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from exceptions import *

import operator
import sys
import time
import re
import pymongo
from pymongo import ASCENDING, DESCENDING
from datetime import datetime
#from pymongo.connection import Connection, _partition_node
#from pymongo.master_slave_connection import MasterSlaveConnection
#from pymongo.errors import AutoReconnect,DuplicateKeyError,CollectionInvalid,OperationFailure

#####################################################################
# COPIED FROM Timeseries module
# TODO: Remove after abstracting the interface for different backends
#####################################################################
if sys.version_info[:2] > (2, 6):
    from collections import OrderedDict
else:
    from ordereddict import OrderedDict

NUMBER_TIME = re.compile('^[\d]+$')
SIMPLE_TIME = re.compile('^([\d]+)([hdwmy])$')

SIMPLE_TIMES = {
  'h' : 60*60,        # hour
  'd' : 60*60*24,     # day
  'w' : 60*60*24*7,   # week
  'm' : 60*60*24*30,  # month(-ish)
  'y' : 60*60*24*365, # year(-ish)
}

def _resolve_time(value):
  '''
  Resolve the time in seconds of a configuration value.
  '''
  if value is None or isinstance(value,(int,long)):
    return value

  if NUMBER_TIME.match(value):
    return long(value)

  simple = SIMPLE_TIME.match(value)
  if SIMPLE_TIME.match(value):
    multiplier = long( simple.groups()[0] )
    constant = SIMPLE_TIMES[ simple.groups()[1] ]
    return multiplier * constant

  raise ValueError('Unsupported time format %s'%value)

class Timeseries(object):
  '''
  Base class of all time series. Also acts as a factory to return the correct
  subclass if "type=" keyword argument supplied.
  '''
  
  def __init__(self, client, **kwargs):
    self._client = client
    self._read_func = kwargs.get('read_func',None)
    self._write_func = kwargs.get('write_func',None)
    self._prefix = kwargs.get('prefix', '')
    self._intervals = kwargs.get('intervals', {})
    if len(self._prefix) and not self._prefix.endswith(':'):
      self._prefix += ':'

    # TODO: allow read/write concerns in kwargs

    # Normalize the handle to a database
    if isinstance(client, pymongo.MongoClient):
      self._client = client['kairos']
    elif isinstance(client, pymongo.database.Database):
      self._client = client
    else:
      raise ValueError('Mongo handle must be MongoClient or database instance')

    # Preprocess the intervals
    for interval,config in self._intervals.iteritems():
      # Re-write the configuration values so that it doesn't have to be
      # processed every time.
      step = config['step'] = _resolve_time( config['step'] ) # Required
      steps = config.get('steps',None)       # Optional
      resolution = config['resolution'] = _resolve_time( 
        config.get('resolution',config['step']) ) # Optional

      def calc_keys(name, timestamp, s=step, r=resolution, i=interval):
        interval_bucket = int( timestamp/s )
        resolution_bucket = int( timestamp/r )
        interval_key = '%s%s:%s:%s'%(self._prefix, name, i, interval_bucket)
        resolution_key = '%s:%s'%(interval_key, resolution_bucket)

        return interval_bucket, resolution_bucket, interval_key, resolution_key
      
      expire = False
      if steps: expire = step*steps

      config['calc_keys'] = calc_keys
      config['expire'] = expire
      config['coarse'] = (resolution==step)

      # Define the indices for lookups and TTLs
      # TODO: the interval+(resolution+)name combination should be unique,
      # but should the index be defined as such? Consider performance vs.
      # correctness tradeoff
      if config['coarse']:
        self._client[interval].ensure_index( 
          [('interval',DESCENDING),('name',ASCENDING)], background=True )
      else:
        self._client[interval].ensure_index( 
          [('interval',DESCENDING),('resolution',DESCENDING),('name',ASCENDING)],
          background=True )
      if expire:
        self._client[interval].ensure_index( 
          [('expire_from',DESCENDING)], expireAfterSeconds=expire, background=True )
  
  def insert(self, name, value, timestamp=None):
    '''
    Insert a value for the timeseries "name". For each interval in the 
    configuration, will insert the value into a bucket for the interval
    "timestamp". If time is not supplied, will default to time.time(), else it
    should be a floating point value.

    This supports the public methods of the same name in the subclasses. The
    value is expected to already be converted 
    '''
    if not timestamp:
      timestamp = time.time()
    if self._write_func:
      value = self._write_func(value)

    for interval,config in self._intervals.iteritems():
      i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)

      insert = {'value':value, 'name':name, 'interval':i_bucket}
      # self._insert( insert )
      if config['coarse']:
        _id = insert['_id'] = i_key
      else:
        _id = insert['_id'] = r_key
        insert['resolution'] = r_bucket

      if config['expire']:
        insert['expire_from'] = datetime.utcfromtimestamp( timestamp )

      self._client[interval].update( {'_id':_id}, insert, upsert=True )

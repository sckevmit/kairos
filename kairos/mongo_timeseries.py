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
      # correctness tradeoff. Also will need to determine if we need to 
      # maintain this multikey index or if there's a better way to implement
      # all the features. Lastly, determine if it's better to add 'value'
      # to the index and spec the fields in get() and series() so that we
      # get covered indices.
      if config['coarse']:
        self._client[interval].ensure_index( 
          [('interval',DESCENDING),('name',ASCENDING)], background=True )
      else:
        self._client[interval].ensure_index( 
          [('interval',DESCENDING),('resolution',ASCENDING),('name',ASCENDING)],
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

    # Mongo does not allow mixing atomic modifiers and non-$set sets in the
    # same update, so the choice is to either run the first upsert on 
    # {'_id':id} to ensure the record is in place followed by an atomic update
    # based on the series type, or use $set for all of the keys to be used in
    # creating the record, which then disallows setting our own _id because
    # that "can't be updated". So the tradeoffs are:
    #   * 2 updates for every insert, maybe a local cache of known _ids and the
    #     associated memory overhead
    #   * Query by the 2 or 3 key tuple for this interval and the associated
    #     overhead of that index match vs. the presumably-faster match on _id
    #   * Yet another index for the would-be _id of i_key or r_key, where each
    #     index has a notable impact on write performance.
    # For now, choosing to go with matching on the tuple until performance 
    # testing can be done. Even then, there may be a variety of factors which
    # make the results situation-dependent.
    # TODO: confirm that this is in fact using the indices correctly.
    for interval,config in self._intervals.iteritems():
      i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)

      insert = {'name':name, 'interval':i_bucket}
      if not config['coarse']:
        insert['resolution'] = r_bucket
      query = insert.copy()

      if config['expire']:
        insert['expire_from'] = datetime.utcfromtimestamp( timestamp )

      # switch to atomic updates
      insert = {'$set':insert.copy()}
      self._insert( insert, value )
      
      # TODO: use write preference settings if we have them
      self._client[interval].update( query, insert, upsert=True, check_keys=False )

  def delete(self, name):
    '''
    Delete time series by name across all intervals. Returns the number of
    records deleted.
    '''
    # TODO: confirm that this does not use the combo index and determine
    # performance implications.
    num_deleted = 0
    for interval,config in self._intervals.iteritems():
      # TODO: use write preference settings if we have them
      num_deleted += self._client[interval].remove( {'name':name} )['n']
    return num_deleted
  
  def get(self, name, interval, timestamp=None, condensed=False, transform=None):
    '''
    Get ze data
    '''
    if not timestamp:
      timestamp = time.time()

    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)
    i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)
    
    rval = OrderedDict()    
    query = {'name':name, 'interval':i_bucket}
    if config['coarse']:
      record = self._client[interval].find_one( query )
      if record:
        data = self._process_row( record['value'] )
        rval[ i_bucket*config['step'] ] = data
      else:
        # TODO: Using same functional tests from redis, this is the expectation.
        # Not sure it's the right assumption though, None seems like the more
        # appropriate value, in which case the key could not be set at all and
        # caller can assume to use get() on the return value.
        rval[ i_bucket*config['step'] ] = []
    else:
      # TODO: this likely needs some optimization, either to force the query 
      # plan, add a range for the resolution condition, or something along
      # those lines.
      sort = [('interval', DESCENDING), ('resolution', ASCENDING) ]
      cursor = self._client[interval].find( spec=query, sort=sort )

      idx = 0
      for record in cursor:
        rval[ record['resolution']*config['resolution'] ] = record['value']

    # If condensed, collapse the result into a single row
    if condensed and not config['coarse']:
      rval = { i_bucket*config['step'] : self._condense(rval) }
    if transform:
      for k,v in rval.iteritems():
        rval[k] = self._transform(v, transform)
    
    return rval

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    raise NotImplementedError()

  def _insert(self, spec, value):
    '''
    Subclasses must implement updating the insert spec with the value given the
    type of timeseries this is.
    '''
    #raise NotImplementedError()
    # series type
    spec['$push'] = {'value':value}
    # count type
    #spec['$inc'] = {'value':value}
    # histogram type
    # spec['$inc'] = {'value.%s'%(value): 1}
    # gauge type
    #spec['$set']['value'] = value
    
  def _get(self, handle, key):
    '''
    Subclasses must implement fetching from a key. Should return the result
    of the call event if handle is a pipeline.
    '''
    raise NotImplementedError()

  def _process_row(self, data):
    '''
    Subclasses should apply any read function to the data. Will only be called
    if there is one.
    '''
    #raise NotImplementedError()
    # series type
    if self._read_func:
      return map(self._read_func, data)
    return data
    # histogram type
    #if not data:
      #return {}
    #rval = {}
    #for value,count in data.iteritems():
      #if self._read_func: value = self._read_func(value)
      #rval[ value ] = int(count)
    #return rval

  def _condense(self, data):
    '''
    Condense a mapping of timestamps and associated data into a single 
    object/value which will be mapped back to a timestamp that covers all
    of the data.
    '''
    # raise NotImplementedError()
    # series type
    if data:
      return reduce(operator.add, data.values())
    return []

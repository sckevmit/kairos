'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from exceptions import *

import operator
import sys
import time
import re

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
  
  def __new__(cls, *args, **kwargs):
    if cls==Timeseries:
      ttype = kwargs.pop('type', None)
      if ttype=='series':
        return Series.__new__(Series, *args, **kwargs)
      elif ttype=='histogram':
        return Histogram.__new__(Histogram, *args, **kwargs)
      elif ttype=='count':
        return Count.__new__(Count, *args, **kwargs)
      elif ttype=='gauge':
        return Gauge.__new__(Gauge, *args, **kwargs)
    return object.__new__(cls, *args, **kwargs)

  def __init__(self, client, **kwargs):
    '''
    Create a time series using a given redis client and keyword arguments
    defining the series configuration. 

    Optionally provide a prefix for all keys. If prefix length>0 and it
    doesn't end with ":", it will be automatically appended.

    The redis client must be API compatible with the Redis instance from
    the redis package http://pypi.python.org/pypi/redis


    The supported keyword arguments are:

    type
      One of (series, histogram, count). Optional, defaults to "series".

      series - each interval will append values to a list
      histogram - each interval will track count of unique values
      count - each interval will maintain a single counter

    prefix
      Optional, is a prefix for all keys in this histogram. If supplied
      and it doesn't end with ":", it will be automatically appended.

    read_func
      Optional, is a function applied to all values read back from the
      database. Without it, values will be strings. Must accept a string
      value and can return anything.

    write_func
      Optional, is a function applied to all values when writing. Can be
      used for histogram resolution, converting an object into an id, etc.
      Must accept whatever can be inserted into a timeseries and return an
      object which can be cast to a string.

    intervals
      Required, a dictionary of interval configurations in the form of: 

      {
        # interval name, used in redis keys and should conform to best practices
        # and not include ":"
        minute: {
          
          # Required. The number of seconds that the interval will cover
          step: 60,

          # Optional. The maximum number of intervals to maintain. If supplied,
          # will use redis expiration to delete old intervals, else intervals
          # exist in perpetuity.
          steps: 240,

          # Optional. Defines the resolution of the data, i.e. the number of 
          # seconds in which data is assumed to have occurred "at the same time".
          # So if you're tracking a month long time series, you may only need 
          # resolution down to the day, or resolution=86400. Defaults to same
          # value as "step".
          resolution: 60,
        }
      }
    '''
    self._client = client
    self._read_func = kwargs.get('read_func',None)
    self._write_func = kwargs.get('write_func',None)
    self._prefix = kwargs.get('prefix', '')
    self._intervals = kwargs.get('intervals', {})
    if len(self._prefix) and not self._prefix.endswith(':'):
      self._prefix += ':'


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

    # TODO: document acceptable names
    # TODO: document what types values are supported
    # TODO: document behavior when time is outside the bounds of step*steps
    # TODO: document how the data is stored.

    pipe = self._client.pipeline(transaction=False)

    for interval,config in self._intervals.iteritems():
      i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)
      
      if config['coarse']:
        #getattr(pipe,func)(i_key, *args)
        self._insert(pipe, i_key, value)
      else:
        # Add the resolution bucket to the interval. This allows us to easily
        # discover the resolution intervals within the larger interval, and
        # if there is a cap on the number of steps, it will go out of scope
        # along with the rest of the data
        pipe.sadd(i_key, r_bucket)
        #getattr(pipe,func)(r_key, *args)
        self._insert(pipe, r_key, value)

      expire = config['expire']
      if expire:
        pipe.expire(i_key, expire)
        if not config['coarse']:
          pipe.expire(r_key, expire)

    pipe.execute()

  def delete(self, name):
    '''
    Delete all data in a timeseries.
    '''
    keys = self._client.keys('%s%s:*'%(self._prefix,name))

    pipe = self._client.pipeline(transaction=False)
    for key in keys:
      pipe.delete( key )
    pipe.execute()

  def get(self, name, interval, timestamp=None, condensed=False, transform=None):
    '''
    Get the set of values for a named timeseries and interval. If timestamp
    supplied, will fetch data for the period of time in which that timestamp
    would have fallen, else returns data for "now". If the timeseries 
    resolution was not defined, then returns a simple list of values for the
    interval, else returns an ordered dict where the keys define the resolution 
    interval and the values are the time series data in that (sub)interval. 
    This allows the user to interpolate sparse data sets.

    If transform is defined, will utilize one of `[mean, count, min, max, sum]`
    to process each row of data returned. If the transform is a callable, will
    pass an array of data to the function. Note that the transform will be run
    after the data is condensed.

    Raises UnknownInterval if `interval` is not one of the configured 
    intervals.

    TODO: Fix this method doc
    '''
    # TODO: support negative values of timestamp as "-N intervals", i.e.
    # -1 on a day interval is yesterday
    if not timestamp:
      timestamp = time.time()

    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)
    i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)
    
    rval = OrderedDict()    
    if config['coarse']:
      data = self._process_row( self._get(self._client, i_key) )
      rval[ i_bucket*config['step'] ] = data
    else:
      # First fetch all of the resolution buckets for this set.
      resolution_buckets = sorted(map(int,self._client.smembers(i_key)))

      # Create a pipe and go fetch all the data for each.
      # TODO: turn off transactions here?
      pipe = self._client.pipeline(transaction=False)
      for bucket in resolution_buckets:
        r_key = '%s:%s'%(i_key, bucket)   # TODO: make this the "resolution_bucket" closure?
        self._get(pipe, r_key)
      res = pipe.execute()

      for idx,data in enumerate(res):
        data = self._process_row(data)
        rval[ resolution_buckets[idx]*config['resolution'] ] = data
    
    # If condensed, collapse the result into a single row
    if condensed and not config['coarse']:
      rval = { i_bucket*config['step'] : self._condense(rval) }
    if transform:
      for k,v in rval.iteritems():
        rval[k] = self._transform(v, transform)
    return rval
  
  def series(self, name, interval, steps=None, condensed=False, timestamp=None, transform=None):
    '''
    Return all the data in a named time series for a given interval. If steps
    not defined and there are none in the config, defaults to 1.

    Returns an ordered dict of interval timestamps to a single interval, which
    matches the return value in get().

    If transform is defined, will utilize one of `[mean, count, min, max, sum]`
    to process each row of data returned. If the transform is a callable, will
    pass an array of data to the function. Note that the transform will be run
    after the data is condensed.

    Raises UnknownInterval if `interval` not configured.
    '''
    # TODO: support start and end timestamps
    # TODO: support other ways of declaring the interval
    if not timestamp:
      timestamp = time.time()

    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)
    step = config.get('step', 1)
    steps = steps if steps else config.get('steps',1)
    resolution = config.get('resolution',step)

    end_timestamp = timestamp
    end_bucket = int( end_timestamp / step )
    start_bucket = end_bucket - steps +1 # +1 because it's inclusive of end

    # First grab all the intervals that matter
      # TODO: use closures on the config for generating this interval key
    pipe = self._client.pipeline(transaction=False)
    rval = OrderedDict()
    for s in range(steps):
      interval_bucket = start_bucket + s
      i_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)
      rval[interval_bucket*step] = OrderedDict()

      if config['coarse']:
        self._get(pipe, i_key)
      else:
        pipe.smembers(i_key)
    res = pipe.execute()

    # TODO: a memory efficient way to use a single pipeline for this.
    for idx,data in enumerate(res):
      # TODO: use closures on the config for generating this interval key
      interval_bucket = start_bucket + idx
      interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

      if config['coarse']:
        data = self._process_row( data )
        if transform:
          data = self._transform(data, transform)
        rval[interval_bucket*step] = data
      else:
        pipe = self._client.pipeline(transaction=False)
        resolution_buckets = sorted(map(int,data))
        for bucket in resolution_buckets:
          # TODO: use closures on the config for generating this resolution key
          resolution_key = '%s:%s'%(interval_key, bucket)
          self._get(pipe, resolution_key)
        
        resolution_res = pipe.execute()
        for x,data in enumerate(resolution_res):
          rval[interval_bucket*step][ resolution_buckets[x]*resolution ] = \
            self._process_row(data)

    # If condensed, collapse each interval into a single value
    if not config['coarse']:
      if condensed:
        for key in rval.iterkeys():
          data = self._condense( rval[key] )
          if transform:
            data = self._transform(data, transform)
          rval[key] = data
      elif transform:
        for interval,resolutions in rval.iteritems():
          for key in resolutions.iterkeys():
            resolutions[key] = self._transform(resolutions[key], transform)
    
    return rval

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    raise NotImplementedError()

  def _insert(self, handle, key, value):
    '''
    Subclasses must implement inserting a value for a key.
    '''
    raise NotImplementedError()
    
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
    raise NotImplementedError()

  def _condense(self, data):
    '''
    Condense a mapping of timestamps and associated data into a single 
    object/value which will be mapped back to a timestamp that covers all
    of the data.
    '''
    raise NotImplementedError()


class Series(Timeseries):
  '''
  Simple time series where all data is stored in a list for each interval.
  '''

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if transform=='mean':
      total = sum( data )
      count = len( data )
      data = float(total)/float(count) if count>0 else 0
    elif transform=='count':
      data = len( data )
    elif transform=='min':
      data = min( data or [0])
    elif transform=='max':
      data = max( data or [0])
    elif transform=='sum':
      data = sum( data )
    elif callable(transform):
      data = transform(data)
    return data

  def _insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.rpush(key, value)

  def _get(self, handle, key):
    return handle.lrange(key, 0, -1)

  def _process_row(self, data):
    if self._read_func:
      return map(self._read_func, data)
    return data

  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    if data:
      return reduce(operator.add, data.values())
    return []

class Histogram(Timeseries):
  '''
  Data for each interval is stored in a hash, counting occurrances of the
  same value within an interval. It is up to the user to determine the precision
  and distribution of the data points within the histogram.
  '''

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if transform=='mean':
      total = sum( k*v for k,v in data.iteritems() )
      count = sum( data.values() )
      data = float(total)/float(count) if count>0 else 0
    elif transform=='count':
      data = sum(data.values())
    elif transform=='min':
      data = min(data.keys() or [0])
    elif transform=='max':
      data = max(data.keys() or [0])
    elif transform=='sum':
      data = sum( k*v for k,v in data.iteritems() )
    elif callable(transform):
      data = reduce( operator.add, ([k]*v for k,v in sorted(data.iteritems())) )
      data = transform(data)
    return data

  def _insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.hincrby(key, value, 1)

  def _get(self, handle, key):
    return handle.hgetall(key)

  def _process_row(self, data):
    rval = {}
    for value,count in data.iteritems():
      if self._read_func: value = self._read_func(value)
      rval[ value ] = int(count)
    return rval
  
  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    rval = {}
    for resolution,histogram in data.iteritems():
      for value,count in histogram.iteritems():
        rval[ value ] = count + rval.get(value,0)
    return rval

class Count(Timeseries):
  '''
  Time series that simply increments within each interval.
  '''

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if callable(transform):
      data = transform(data)
    return data
  
  def insert(self, name, value=1, timestamp=None):
    super(Count,self).insert(name, value, timestamp)

  def _insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    if value!=0:
      if isinstance(value,float):
        handle.incrbyfloat(key, value)
      else:
        handle.incr(key,value)
  
  def _get(self, handle, key):
    return handle.get(key)

  def _process_row(self, data):
    return int(data) if data else 0

  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    if data:
      return sum(data.values())
    return 0

class Gauge(Timeseries):
  '''
  Time series that stores the last value.
  '''

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if callable(transform):
      data = transform(data)
    return data
  
  def _insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.set(key, value)
  
  def _get(self, handle, key):
    return handle.get(key)

  def _process_row(self, data):
    return data

  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    if data:
      return data.values()
    return []

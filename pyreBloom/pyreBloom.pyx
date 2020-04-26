# Copyright (c) 2011 SEOmoz
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#cython: language_level=3

import math
import random

cimport bloom

from libc.stdint cimport uint32_t

class pyreBloomException(Exception):
    '''Some sort of exception has happened internally'''
    pass


cdef class pyreBloom(object):
    cdef bloom.pyrebloomctxt context
    cdef bytes               key
    cdef uint32_t			 chunk_size

    property bits:
        def __get__(self):
            return self.context.bits

    property hashes:
        def __get__(self):
            return self.context.hashes

    def __cinit__(self, key, capacity, error, host=b'127.0.0.1', port=6379,
        password=b'', db=0, count=False):
        self.key = key
        if bloom.init_pyrebloom(&self.context, self.key, capacity,
            error, host, port, password, db, count):
            raise pyreBloomException(self.context.errstr)
        self.chunk_size = int(10000/self.hashes)

    def __dealloc__(self):
        bloom.free_pyrebloom(&self.context)

    def delete(self):
        bloom.delete(&self.context)
    
    def get_item_num(self):
        if bloom.get_counter(&self.context):
            raise pyreBloomException(self.context.errstr)
        return self.context.counter_value

    def put(self, value):	
        if isinstance(value, bytes):
            bloom.add(&self.context, value, len(value))
            r = bloom.add_complete(&self.context, 1)
        elif isinstance(value, (list, tuple)):
            for v in value:
                bloom.add(&self.context, v, len(v))
            r = bloom.add_complete(&self.context, len(value))
        else:
            raise Exception("unsupport value type, suport bytes or a list of bytes")
        
        if r < 0:
            raise pyreBloomException(self.context.errstr)
        if self.context.count:
            bloom.incr_counter(&self.context, r)
        return r

    def add(self, value):
        return self.put(value)

    def extend(self, values):
        ## WARNING: keep the size of pipeline to chunk_size
        counter = 0
        groups = (values[i:i + self.chunk_size] for i in range(0, len(values), self.chunk_size))
        for group in groups:
            counter += self.put(group)
        return counter

    def contains(self, value):
        if isinstance(value, bytes):
            bloom.check(&self.context, value, len(value))
            r = bloom.check_next(&self.context)
            if (r < 0):
                raise pyreBloomException(self.context.errstr)
            return bool(r)
        # If the object is 'iterable'...
        elif isinstance(value, (list, tuple)):
            results = []
            groups = (value[i:i + self.chunk_size] for i in range(0, len(value), self.chunk_size))
            for group in groups:
                for v in group:
                    bloom.check(&self.context, v, len(v))
                r = [bloom.check_next(&self.context) for i in range(len(group))]
                if (r and min(r) < 0):
                    raise pyreBloomException(self.context.errstr)
                for v, included in zip(group, r):
                    if included:
                        results.append(v)
            return results
        else:
            raise Exception("unsupport value type, suport bytes or a list of bytes")

    def ncontains(self, value):
        if isinstance(value, bytes):
            bloom.check(&self.context, value, len(value))
            r = bloom.check_next(&self.context)
            if (r < 0):
                raise pyreBloomException(self.context.errstr)
            return not bool(r)
        # If the object is 'iterable'...
        elif isinstance(value, (list, tuple)):
            results = []
            groups = (value[i:i + self.chunk_size] for i in range(0, len(value), self.chunk_size))
            for group in groups:
                for v in group:
                    bloom.check(&self.context, v, len(v))
                r = [bloom.check_next(&self.context) for i in range(len(group))]
                if (r and min(r) < 0):
                    raise pyreBloomException(self.context.errstr)
                for v, included in zip(group, r):
                    if not included:
                        results.append(v)
            return results
        else:
            raise Exception("unsupport value type, suport bytes or a list of bytes")

    def __contains__(self, value):
        return self.contains(value)

    def keys(self):
        '''Return a list of the keys used in this bloom filter'''
        return [self.context.keys[i] for i in range(self.context.num_keys)]

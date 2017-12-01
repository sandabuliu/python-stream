#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import multiprocessing

__author__ = 'tong'


def start_process(func):
    proc = multiprocessing.Process(target=func)
    proc.start()
    return proc


def endpoint(fp):
    pos = fp.tell()
    fp.seek(0, 2)
    end = fp.tell()
    fp.seek(pos)
    return end


class Window(object):
    def __init__(self, size=None, timeout=None):
        self.size = size
        self.timer = time.time()
        self.buffer = []
        self.timeout = timeout

    @property
    def empty(self):
        return not bool(self.buffer)

    @property
    def data(self):
        data = self.buffer
        self.buffer = []
        return data
        
    def append(self, item):
        if not self.buffer:
            self.timer = time.time()
        self.buffer.append(item)

    @property
    def fulled(self):
        if not self.size and not self.timeout:
            return False
        if not self.buffer:
            return False
        if self.size:
            if len(self.buffer) >= self.size:
                return True
            if not self.timeout:
                return False
        return time.time() - self.timer >= self.timeout


def ifilter(name, path, **kwargs):
    if name == 'bloom':
        return BloomFilter(path, **kwargs)
    if name == 'max':
        return MaxFilter(path, **kwargs)


def gzip(item):
    import gzip
    import StringIO
    s = StringIO.StringIO()
    data = gzip.GzipFile(fileobj=s, mode='w')
    data.write(item)
    data.close()
    return s.getvalue()


def ungzip(item):
    import gzip
    import StringIO
    s = StringIO.StringIO()
    data = gzip.GzipFile(fileobj=s, mode='w')
    data.write(item)
    data.close()
    return s.getvalue()


class IterableError(Exception):
    pass


class BloomFilter(object):
    def __init__(self, cachefile, capacity=1000000, error_rate=0.001):
        self.cachefile = cachefile
        if os.name == 'nt' or not cachefile:
            from pybloom import BloomFilter
            if self.cache():
                with open(cachefile, 'r') as fp:
                    self.filter = BloomFilter.fromfile(fp)
            else:
                self.filter = BloomFilter(capacity=capacity, error_rate=error_rate)
        elif os.name == 'posix':
            from pybloomfilter import BloomFilter
            if self.cache():
                self.filter = BloomFilter.open(self.cachefile)
            else:
                self.filter = BloomFilter(capacity, error_rate, cachefile)

    def __contains__(self, key):
        return key in self.filter

    def add(self, obj):
        self.filter.add(obj)
        if os.name == 'nt':
            with open(self.cachefile, 'w') as fp:
                self.filter.tofile(fp)

    def cache(self):
        return os.path.exists(self.cachefile or '')


class MaxFilter(object):
    def __init__(self, cachefile, is_number=False):
        self.cachefile = cachefile
        self.is_number = is_number
        if os.path.exists(self.cachefile or ''):
            with open(self.cachefile, 'w') as fp:
                text = fp.read()
                self.max_value = float(text) if self.is_number else text
        else:
            self.max_value = None

    def __contains__(self, key):
        if self.is_number:
            key = float(key)
        return key <= self.max_value

    def add(self, obj):
        self.max_value = obj
        with open(self.cachefile, 'w') as fp:
            fp.write(self.max_value)

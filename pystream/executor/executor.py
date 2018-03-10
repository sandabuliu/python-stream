#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import json
import msgpack
import logging

from .event import Event, is_event
from .utils import Window, gzip, ungzip
from ..logic import Key, Or, And
from ..utils import DefaultParser
from ..logparser.logparser import LogParser

__author__ = 'tong'

logger = logging.getLogger('stream.logger')


class Executor(object):
    def __init__(self, name=None, ignore_exc=True, **kwargs):
        self.name = name or ('%s-%s' % (self.__class__.__name__, id(self)))
        self.ignore_exc = ignore_exc
        self._source = None
        self._output = None
        self.kwargs = kwargs

    def __iter__(self):
        for item in self.source:
            try:
                if is_event(item):
                    if item == Event.SKIP:
                        continue
                    result = self.handle_event(item)
                    if result is not None:
                        yield result
                    if self._output:
                        yield item
                    continue

                result = self.handle(item)
                if result is not None:
                    yield result
            except BaseException, e:
                if self.ignore_exc:
                    self.handle_exception(item, e)
                else:
                    raise

    def handle(self, item):
        return item

    def handle_exception(self, item, e):
        logger.warn('%s (%s) handled failed, cause: %s, data: %s' % (self.name, self.__class__.__name__, e, [item]))

    def handle_event(self, event):
        return None

    def start(self):
        for _ in self:
            continue

    def __or__(self, executor):
        source = executor             # type: Executor
        while source._source:
            source = source._source
        source._source = self
        self._output = source
        return executor

    @property
    def source(self):
        if not self._source:
            raise Exception('Lack of data source!')
        return self._source


class Parser(Executor):
    def __init__(self, rule=None, trace=False, **kwargs):
        super(Parser, self).__init__(**kwargs)
        self.trace = trace
        self.parser = LogParser(rule) if rule else DefaultParser()

    def handle(self, item):
        if not item:
            return item
        result = self.parser.parse(item)
        if not result:
            return result
        return result.trace() if self.trace else result.result()

    def handle_exception(self, item, e):
        logger.warn(
            'PARSER PASS %s %s error: %s' % (self.name, type(e).__name__, e),
            extra={'extra': dict(line=item.strip(), type=e.type, rule=e.rule, data=e.line)}
        )


class Filter(Executor):
    def __init__(self, *args, **kwargs):
        super(Filter, self).__init__(**kwargs)
        if any([not isinstance(i, (Key, Or, And)) for i in args]):
            raise Exception('Filter args should be in (`Field`, `Or`, `And`)')
        if not args:
            self.filter = type('name'.encode('utf8'), (object, ), {'result': lambda s, x: True})()
        elif len(args) == 1:
            self.filter = args[0]
        else:
            self.filter = And(*args)

    def handle(self, item):
        if self.filter.result(item):
            return item
        else:
            return None

    def handle_exception(self, item, e):
        logger.warn('Filter %s( %s ) Failed, cause: %s' % (self.name, str(self.filter), e))


class Map(Executor):
    def __init__(self, function, **kwargs):
        super(Map, self).__init__(**kwargs)
        self.func = function

    def handle(self, item):
        return self.func(item)
    

class Sort(Executor):
    def __init__(self, maxlen=None, key=None, desc=False, maxsize=None, cache_path=None, **kwargs):
        super(Sort, self).__init__(**kwargs)
        self.maxlen = maxlen
        self.maxsize = maxsize
        self.items = []
        self.files = []
        self.length = 0
        self.key = key or (lambda x: x)
        self.desc = desc
        self.cache_path = cache_path or '/tmp/pystream_sort_%s' % time.time()

    def handle(self, item):
        self.insert(item)
        if self.maxlen:
            if len(self.items) > self.maxlen:
                self.items = self.items[:self.maxlen]
        elif self.maxsize and self.length > self.maxsize:
            if not os.path.exists(self.cache_path):
                os.makedirs(self.cache_path)
            filename = os.path.join(self.cache_path, str(time.time()))
            fp = open(filename, 'w')
            fp.write('\n'.join([_ for _ in self.items]))
            fp.close()
            self.files.append(filename)
            self.length = 0
            self.items = []
        return self.Iterable(self)

    def insert(self, item):
        l = 0
        r = len(self.items)-1
        compare = (lambda x, y: x > y) if self.desc else (lambda x, y: x < y)
        while l <= r:
            m = (l + r)/2
            value1 = self.key(self.items[m])
            value2 = self.key(item)
            if value1 == value2:
                break
            elif compare(value1, value2):
                l = m + 1
            else:
                r = m - 1
        self.items.insert((l + r)/2+1, item)
        if self.maxsize:
            self.length += len(item)

    class Iterable(object):
        def __init__(self, exe):
            self.exe = exe
            self.func = max if exe.desc else min

        def __iter__(self):
            if not self.exe.maxsize:
                for item in self.exe.items:
                    yield item
                return
            fps = [iter(self.exe.items)] if self.exe.items else []
            fps += [open(_) for _ in self.exe.files]
            fps = [[_, next(_).strip('\n')] for _ in fps]
            while fps:
                item = self.func(fps, key=(lambda x: self.exe.key(x[1])))
                yield item[1]
                try:
                    item[1] = next(item[0]).strip('\n')
                except StopIteration:
                    fps.remove(item)


class Reduce(Executor):
    def __init__(self, function, **kwargs):
        super(Reduce, self).__init__(**kwargs)
        self.func = function
        self.data = None

    def handle(self, item):
        if self.data is None:
            self.data = item
        else:
            self.data = self.func(self.data, item)
        return self.data


class ReducebyKey(Executor):
    def __init__(self, function, **kwargs):
        super(ReducebyKey, self).__init__(**kwargs)
        self.func = function
        self.data = {}

    def handle(self, item):
        key, item = item
        if key not in self.data:
            self.data[key] = item
        else:
            self.data[key] = self.func(self.data[key], item)
        return key, self.data[key]


class Group(Executor):
    def __init__(self, function=None, size=None, timeout=None, window=None, **kwargs):
        super(Group, self).__init__(**kwargs)
        self.func = function or (lambda x: x)
        if window:
            self.window = window
        else:
            self.window = Window(size, timeout)

    def __iter__(self):
        for item in super(Group, self).__iter__():
            yield item
        if not self.window.empty:
            yield self.func(self.window.data)

    def handle_event(self, event):
        if event == Event.IDLE:
            if self.window.fulled:
                return self.func(self.window.data)

    def handle(self, item):
        self.window.append(item)
        if self.window.fulled:
            return self.func(self.window.data)
        return None


class JsonDumps(Executor):
    def handle(self, item):
        return json.dumps(item, **self.kwargs)


class JsonLoads(Executor):
    def handle(self, item):
        return json.loads(item, **self.kwargs)


class MsgpackDumps(Executor):
    def handle(self, item):
        return msgpack.dumps(item, **self.kwargs)


class MsgpackLoads(Executor):
    def handle(self, item):
        return msgpack.loads(item, **self.kwargs)


class GZip(Executor):
    def handle(self, item):
        return gzip(item)


class UnGZip(Executor):
    def handle(self, item):
        return ungzip(item)


class Encode(Executor):
    def __init__(self, encoding, **kwargs):
        super(Encode, self).__init__(**kwargs)
        self.encoding = encoding

    def handle(self, item):
        return item.encode(self.encoding)


class Decode(Executor):
    def __init__(self, encoding, **kwargs):
        super(Decode, self).__init__(**kwargs)
        self.encoding = encoding

    def handle(self, item):
        return item.decode(self.encoding)


class Iterator(Executor):
    def __init__(self, function=None, **kwargs):
        super(Iterator, self).__init__(**kwargs)
        self.func = function or (lambda x: x)

    def __iter__(self):
        iterator = super(Iterator, self).__iter__()
        for items in iterator:
            if is_event(items) and self._output:
                yield items
                continue
            for item in items:
                yield self.func(item)

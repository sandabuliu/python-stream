#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import msgpack
import logging

from .event import Event, is_event
from .utils import Window, gzip, ungzip
from ..logic import Key, Or, And
from ..utils import DefaultParser, JSONCls
from ..logparser.logparser import LogParser

__author__ = 'tong'

logger = logging.getLogger('stream.logger')


class Executor(object):
    def __init__(self, name=None, ignore_exc=True, **kwargs):
        self.name = name or ('%s-%s' % (self.__class__.__name__, id(self)))
        self.ignore_exc = ignore_exc
        self._source = None
        self._output = None

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


class Reduce(Executor):
    def __init__(self, function, size=None, timeout=None, window=None, **kwargs):
        super(Reduce, self).__init__(**kwargs)
        self.func = function
        if window:
            self.window = window
        else:
            self.window = Window(size, timeout)

    def __iter__(self):
        for item in super(Reduce, self).__iter__():
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
        return json.dumps(item, cls=JSONCls)


class JsonLoads(Executor):
    def handle(self, item):
        return json.loads(item)


class MsgpackDumps(Executor):
    def handle(self, item):
        return msgpack.dumps(item)


class MsgpackLoads(Executor):
    def handle(self, item):
        return msgpack.loads(item)


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

                for result in self.handle(item):
                    yield result
            except BaseException, e:
                if self.ignore_exc:
                    self.handle_exception(item, e)
                else:
                    raise

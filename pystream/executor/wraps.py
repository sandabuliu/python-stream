#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import traceback

from output import Output
from executor import Executor
from utils import start_process
from event import Event, is_event


__author__ = 'tong'

logger = logging.getLogger('stream.logger')


class Wraps(Executor):
    def __or__(self, other):
        raise Exception('please used by `source | %s(exe)`' % self.__class__.__name__)


class Batch(Wraps):
    def __init__(self, sender, **kwargs):
        if not isinstance(sender, Output):
            raise Exception('sender only accept `Ouput` type')
        self.sender = sender
        super(Batch, self).__init__(**kwargs)

    def handle(self, item):
        try:
            self.sender.outputmany(item)
        except Exception, e:
            logger.error('OUTPUT %s %s error: %s' % (self.__class__.__name__, self.name, e))
            return {'data': item, 'exception': e, 'traceback': traceback.format_exc()}


class Combiner(Wraps):
    def __init__(self, *args, **kwargs):
        super(Combiner, self).__init__(**kwargs)
        self._source = args
        if not args:
            raise Exception('no source')

    @property
    def source(self):
        sources = [iter(s) for s in self._source]
        while True:
            if not sources:
                break
            source = sources.pop(0)
            try:
                while True:
                    item = next(source)
                    yield item
                    if is_event(item) and item == Event.IDLE:
                        break
                sources.append(source)
            except StopIteration:
                pass


class Daemonic(Wraps):
    def __init__(self, exe, handler=None, **kwargs):
        super(Daemonic, self).__init__(**kwargs)
        self.exe = exe
        self.handler = handler

    def __iter__(self):
        raise Exception('please use `start` method')

    def _run(self):
        if isinstance(self.exe, Output):
            handle = self.handler or (lambda x: self.handle_exception(x['data'], x['exception']))
        else:
            handle = self.handler or (lambda x: x)
        for item in self.exe:
            handle(item)

    def start(self):
        if hasattr(self.exe, 'start'):
            return start_process(self.exe.start)
        else:
            return start_process(self._run)


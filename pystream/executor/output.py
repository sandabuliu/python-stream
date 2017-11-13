#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import traceback
from executor import Executor


__author__ = 'tong'

logger = logging.getLogger('stream.logger')


class Output(Executor):
    def handle(self, item):
        try:
            self.output(item)
        except Exception, e:
            logger.error('OUTPUT %s %s error: %s' % (self.__class__.__name__, self.name, e))
            return {'data': item, 'exception': e, 'traceback': traceback.format_exc()}

    def output(self, item):
        pass

    def outputmany(self, items):
        for item in items:
            self.output(item)


class Kafka(Output):
    def __init__(self, topic, server, client=None, name=None, ignore_exc=None, **kwargs):
        try:
            import kafka
        except ImportError:
            raise Exception('Lack of kafka module, try to execute `pip install kafka-python>=1.3.1` install it')

        client = client or kafka.SimpleClient
        self._producer = None
        self._topic = topic
        try:
            self._kafka = client(server, **kwargs)
        except Exception, e:
            raise Exception('kafka client init failed: %s' % e)
        self.producer(kafka.SimpleProducer)
        super(Kafka, self).__init__(name, ignore_exc)

    def producer(self, producer, **kwargs):
        try:
            self._producer = producer(self._kafka, **kwargs)
        except Exception, e:
            raise Exception('kafka producer init failed: %s' % e)

    def output(self, item):
        if not self._producer:
            raise Exception('No producer init')
        logger.info('OUTPUT INSERT Kafka 1: %s' % self._producer.send_messages(self._topic, item))

    def outputmany(self, items):
        if not self._producer:
            raise Exception('No producer init')
        logger.info('OUTPUT INSERT Kafka %s: %s' % (len(items), self._producer.send_messages(self._topic, *items)))

    def close(self):
        if self._producer:
            del self._producer
            self._producer = None


class HTTPRequest(Output):
    def __init__(self, server, headers=None, method='GET', **kwargs):
        from .. import __version__
        self.server = server
        self.method = method.upper()
        self.headers = headers or {}
        self.headers.setdefault('User-Agent', 'python-stream %s HTTPRequest' % __version__)
        super(HTTPRequest, self).__init__(**kwargs)

    def output(self, item):
        import requests
        if self.method == 'GET':
            ret = requests.get(self.server, params=item, headers=self.headers)
            logger.info('OUTPUT INSERT Request 1: %s' % ret)
        elif self.method == 'POST':
            ret = requests.post(self.server, data=self.data(item), headers=self.headers)
            logger.info('OUTPUT INSERT Request 1: %s' % ret)

    def data(self, data):
        ctype = self.headers.get('Content-Type')
        if ctype == 'application/json':
            return json.dumps(data, separators=(',', ':'))
        return data


class Screen(Output):
    def output(self, item):
        print item


class Null(Output):
    pass

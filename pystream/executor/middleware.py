#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import socket
import logging
import multiprocessing
from random import randint
from collections import defaultdict
from asyncore import dispatcher, dispatcher_with_send

from async import TCPClient
from source import Socket
from utils import Window, start_process
from executor import Executor, Reduce, Iterator


__author__ = 'tong'
__all__ = ['Queue', 'Subscribe']

logger = logging.getLogger('stream.logger')


class Queue(Executor):
    EOF = type('EOF', (object, ), {})()

    def __init__(self, batch=None, timeout=None, qsize=1000, **kwargs):
        super(Queue, self).__init__(**kwargs)
        self.qsize = qsize
        self.timeout = timeout or 2
        self.batch = batch

    def run(self, *args):
        exe, callback = args
        for item in exe:
            callback(item)
        callback(self.EOF)

    @property
    def source(self):
        import source
        pipe = multiprocessing.Queue(self.qsize)
        sc = super(Queue, self).source
        rc = Reduce(lambda x: x, window=Window(self.batch, self.timeout))
        p = multiprocessing.Process(target=self.run, args=(sc | rc, pipe.put))
        p.daemon = True
        p.start()
        return source.Queue(pipe, EOF=self.EOF) | Iterator()


class Subscribe(Executor):
    def __init__(self, address=None, cache_path=None, maxsize=1024*1024, listen_num=5, **kwargs):
        self.mutex = multiprocessing.Lock()
        self.sensor = None
        self.server = None
        self.maxsize = maxsize
        self.listen_num = listen_num
        self.cache_path = cache_path or '/tmp/pystream_data_%s' % time.time()
        if hasattr(socket, 'AF_UNIX'):
            self.address = address or '/tmp/pystream_sock_%s' % time.time()
        else:
            self.address = address or ('127.0.0.1', randint(20000, 50000))
        if os.path.exists(self.cache_path):
            os.mkdir(self.cache_path)
        super(Subscribe, self).__init__(**kwargs)

    def init_sensor(self):
        server = self._source | self.producer
        server.start()

    @property
    def producer(self):
        return Sensor(self.address)

    def start(self):
        import asyncore
        if self._source and not self.sensor:
            self.sensor = start_process(self.init_sensor)
        TCPServer(self.address, self.cache_path, self.maxsize, self.listen_num)
        asyncore.loop()

    def __iter__(self):
        raise Exception('please use `[]` to choose topic')

    def __getitem__(self, item):
        class Unpack(Iterator):
            def handle(self, _):
                if 'history' in _:
                    for filename in _['history']:
                        with open(filename) as fp:
                            yield fp.readline()
                for data in _.get('data', []):
                    yield data

        item += '\n'
        s = Socket(self.address, lambda: item,
                   lambda _: _.send('0'), json.loads,
                   lambda x: not x.get('data'))
        u = Unpack()
        return s | u


class Sensor(TCPClient):
    def handle_connect(self):
        self.send('1')

    def handle_read(self):
        self.recv(3)

    def handle_write(self):
        self.message = ','.join(self.message)
        TCPClient.handle_write(self)


class TCPServer(dispatcher):
    def __init__(self, address, path, maxsize=1024*1024, listen_num=5):
        dispatcher.__init__(self)
        socket_af = socket.AF_UNIX if isinstance(address, basestring) else socket.AF_INET
        self.create_socket(socket_af, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(address)
        self.listen(listen_num)
        self.size = maxsize
        self.path = path
        self.data = defaultdict(list)
        self.publishers = defaultdict(dict)

    def location(self):
        for topic, item in self.data.items():
            path = os.path.join(self.path, topic)
            if not os.path.exists(path):
                os.makedirs(path)
                open(os.path.join(path, '0'), 'w').close()
            filenum = max([int(_) for _ in os.listdir(path)])
            filesize = os.path.getsize(os.path.join(path, str(filenum)))
            if filesize + sys.getsizeof(item) > 1024 * 1024 * 1024:
                fp = open(os.path.join(path, str(filenum + 1)), 'a')
            else:
                fp = open(os.path.join(path, str(filenum)), 'a')
            fp.write('\n'.join(item)+'\n')
        self.data.clear()

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            htype = None
            logger.info('server connect to %s(%s), pid: %s' % (addr, sock.fileno(), os.getpid()))
            while not htype:
                try:
                    htype = sock.recv(1)
                except socket.error:
                    time.sleep(1)
            logger.info('server connect to %s(%s), type: %s' % (addr, sock.fileno(), htype))
            if htype == '1':
                PutHandler(self, sock)
            elif htype == '0':
                GetHandler(self, sock)
            else:
                sock.close()

    def items(self, topic):
        path = os.path.join(self.path, topic)
        if os.path.exists(path):
            history = [os.path.join(self.path, str(_)) for _ in
                       sorted([int(_) for _ in os.listdir(path)])]
        else:
            history = []
        data = self.data[topic]
        return {'data': data, 'history': history}

    def handle_error(self):
        logger.error('server socket %s error' % str(self.addr))
        self.handle_close()

    def handle_expt(self):
        logger.error('server socket %s error: unhandled incoming priority event' % str(self.addr))

    def handle_close(self):
        logger.info('server socket %s close' % str(self.addr))
        self.close()


class Handler(dispatcher_with_send):
    def __init__(self, server, *args):
        dispatcher_with_send.__init__(self, *args)
        self.server = server
        self.message = ''

    def handle_error(self):
        logger.error('server handler socket %s error' % str(self.addr))
        self.handle_close()

    def handle_expt(self):
        logger.error('server handler socket %s error: unhandled incoming priority event' % str(self.addr))

    def handle_close(self):
        logger.info('server(%s) socket %s close' % (self.__class__.__name__, self.addr))
        self.close()

    def handle_read(self):
        while True:
            try:
                msg = self.recv(512)
                if not msg:
                    break
                self.message += msg
            except socket.error, e:
                if e.errno == 35:
                    break
                else:
                    raise

        if '\n' not in self.message:
            data = ''
        else:
            data, self.message = self.message.rsplit('\n', 1)

        for item in data.split('\n'):
            if item:
                self.send(self.handle(item) + '\n')


class PutHandler(Handler):
    def handle(self, data):
        topic, data = data.split(',', 1)
        self.server.data[topic].append(data)
        if topic in self.server.publishers:
            for key in self.server.publishers[topic]:
                self.server.publishers[topic][key].append(data)
        if sys.getsizeof(self.server.data) >= self.server.size:
            self.server.location()
        return '200'


class GetHandler(Handler):
    def handle(self, data):
        topic = data.strip()
        if id(self) not in self.server.publishers[topic]:
            self.server.publishers[topic][id(self)] = []
            return json.dumps(self.server.items(topic))
        items = self.server.publishers[topic][id(self)]
        self.server.publishers[topic][id(self)] = []
        return json.dumps({'data': items})

    def handle_close(self):
        for key in self.server.publishers:
            self.server.publishers[key].pop(id(self), None)
        Handler.handle_close(self)

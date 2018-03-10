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
from asyncore import dispatcher

from async import TCPClient
from utils import Window, start_process, endpoint
from executor import Executor, Group, Iterator


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
        rc = Group(window=Window(self.batch, self.timeout))
        p = multiprocessing.Process(target=self.run, args=(sc | rc, pipe.put))
        p.daemon = True
        p.start()
        iterator = Iterator()
        iterator._output = self._output
        return source.Queue(pipe, EOF=self.EOF) | iterator


class Subscribe(Executor):
    def __init__(self, address=None, cache_path=None, maxsize=1024*1024,
                 listen_num=5, archive_size=1024*1024*1024, **kwargs):
        self.mutex = multiprocessing.Lock()
        self.sensor = None
        self.server = None
        self.maxsize = maxsize
        self.listen_num = listen_num
        self.archive_size = archive_size or 1024*1024*1024
        self.cache_path = cache_path or '/tmp/pystream_data_%s' % time.time()
        if hasattr(socket, 'AF_UNIX'):
            self.address = address or '/tmp/pystream_sock_%s' % time.time()
        else:
            self.address = address or ('127.0.0.1', randint(20000, 50000))
        if not os.path.exists(self.cache_path):
            os.mkdir(self.cache_path)
        super(Subscribe, self).__init__(**kwargs)

    def init_sensor(self):
        try:
            server = self._source | self.producer
            server.start()
        except Exception, e:
            logger.error('sensor error: %s' % e)

    @property
    def producer(self):
        return Sensor(self.address)

    def start(self):
        import asyncore
        if self._source and not self.sensor:
            self.sensor = start_process(self.init_sensor)
        TCPServer(self.address, self.cache_path, self.maxsize, self.listen_num, self.archive_size)
        asyncore.loop()

    def status(self, topic):
        return self._get('topic %s' % topic)

    def stop(self):
        return self._get('stop')

    @property
    def topics(self):
        return self._get('topics')

    def __iter__(self):
        raise Exception('please use `[]` to choose topic')

    def __getitem__(self, name):
        import source

        class Receiver(source.TCPClient):
            def initialize(self):
                sock = super(Receiver, self).initialize()
                if isinstance(name, basestring):
                    sock.send('0{"topic": "%s"}\n' % name)
                elif len(name) == 2:
                    sock.send('0{"topic": "%s", "number": %s}\n' % name)
                else:
                    sock.send('0{"topic": "%s", "number": %s, "offset": %s}\n' % name)
                return sock

        class Mapper(Executor):
            def __init__(self, **kwargs):
                super(Mapper, self).__init__(**kwargs)
                self.file = None
                self.position = None

            def handle(self, item):
                if not item:
                    return None
                self.file, self.position, item = item.split('#', 2)
                return item

        s = Receiver(self.address)
        u = Mapper()
        return s | u

    def _get(self, text):
        socket_af = socket.AF_UNIX if isinstance(self.address, basestring) else socket.AF_INET
        sock = socket.socket(socket_af, socket.SOCK_STREAM)
        sock.connect(self.address)
        sock.send('2%s\n' % text.strip())
        fp = sock.makefile('r')
        res = fp.readline().strip()
        while not res:
            res = fp.readline().strip()
        sock.close()
        if res:
            return json.loads(res)
        return None


class Sensor(TCPClient):
    def handle_connect(self):
        self.send('1')

    def handle_read(self):
        self.recv(3)

    def handle_write(self):
        self.message = ','.join(self.message)
        TCPClient.handle_write(self)


class TCPServer(dispatcher):
    def __init__(self, address, path, maxsize=1024*1024, listen_num=5, archive_size=1024*1024*1024):
        dispatcher.__init__(self)
        socket_af = socket.AF_UNIX if isinstance(address, basestring) else socket.AF_INET
        self.create_socket(socket_af, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(address)
        self.listen(listen_num)
        self.size = maxsize
        self.archive_size = archive_size
        self.path = path
        self.data = {}
        self.files = {}

    def location(self, topic):
        filenum, fp = self.files[topic]
        path = os.path.join(self.path, topic)
        items = self.data[topic]
        filesize = os.path.getsize(os.path.join(path, str(filenum)))
        size = 0
        if filesize + sum([len(_) for _ in items]) > self.archive_size:
            fp.close()
            fp = open(os.path.join(path, str(filenum+1)), 'a')
            self.files[topic] = [filenum+1, fp]
        for item in items:
            pos = fp.tell()
            fp.write('%s#%s#%s\n' % (self.files[topic][0], pos, item))
            size += fp.tell() - pos
        self.data[topic] = []
        logger.info('SUBSCRIBE topic [%s] location %s successfully' % (topic, size))

    def topic(self, name):
        if name in self.data:
            return self.data[name]
        self.data[name] = []
        path = os.path.join(self.path, name)
        if not os.path.exists(path):
            os.makedirs(path)
            open(os.path.join(path, '0'), 'w').close()
        filenum = max([int(_) for _ in os.listdir(path)])
        self.files[name] = (filenum, open(os.path.join(path, str(filenum)), 'a'))
        return self.data[name]

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            htype = None
            logger.info('server connect to %s(%s), pid: %s' % (addr, sock.fileno(), os.getpid()))
            counter = 0
            while not htype:
                try:
                    sock.send('\n')
                    htype = sock.recv(1)
                except socket.error:
                    time.sleep(1)
                    counter += 1
                if counter > 5:
                    break
            logger.info('server connect to %s(%s), type: %s' % (addr, sock.fileno(), htype))
            if htype == '0':
                GetHandler(self, sock)
            elif htype == '1':
                PutHandler(self, sock)
            elif htype == '2':
                StaHandler(self, sock)
            else:
                sock.close()

    def handle_error(self):
        logger.error('server socket %s error' % str(self.addr))
        self.handle_close()

    def handle_expt(self):
        logger.error('server socket %s error: unhandled incoming priority event' % str(self.addr))

    def handle_close(self):
        logger.info('server socket %s close' % str(self.addr))
        self.close()


class Handler(dispatcher):
    def __init__(self, server, *args):
        dispatcher.__init__(self, *args)
        self.server = server
        self.message = ''

    def handle_error(self, e=None):
        logger.error('server handler socket %s error: %s' % (str(self.addr), e))
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

        try:
            for item in data.split('\n'):
                if item:
                    self.send(self.handle(item) + '\n')
        except Exception, e:
            self.handle_error(e)


class PutHandler(Handler):
    def handle(self, data):
        topic, data = data.split(',', 1)
        self.server.topic(topic).append(data)
        if sys.getsizeof(self.server.data[topic]) >= self.server.size:
            self.server.location(topic)
        return '200'


class StaHandler(Handler):
    def topics(self):
        keys = self.server.data.keys()
        if os.path.exists(self.server.path):
            keys += os.listdir(self.server.path)
        return json.dumps(list(set(keys)))

    def status(self, name):
        ret = {}
        if os.path.exists(self.server.path):
            filepath = os.path.join(self.server.path, name)
            if os.path.exists(filepath):
                files = os.listdir(filepath)
                ret['filenum'] = len(files)
                ret['filesize'] = sum([os.path.getsize(os.path.join(filepath, _)) for _ in files])
            items = self.server.data.get(name, [])
            ret['memsize'] = sum([len(_) for _ in items])
        return json.dumps(ret)

    def stop(self):
        for name in self.server.data:
            self.server.location(name)
        self.server.close()
        for handler in self._map.values():
            if isinstance(handler, PutHandler):
                handler.close()
        return 'true'

    def handle(self, data):
        data = data.strip().lower()
        if data == 'topics':
            return self.topics()
        if data.startswith('topic '):
            name = data[6:]
            return self.status(name)
        if data == 'stop':
            return self.stop()
        return 'null'


class GetHandler(Handler):
    def __init__(self, server, *args):
        Handler.__init__(self, server, *args)
        self.topic = None
        self.number = -1
        self.blocksize = 0
        self.offset = 0
        self.fp = None

    def handle(self, data):
        data = json.loads(data)
        self.topic = data['topic']
        if data.get('number'):
            self.use(data['number'])
        if data.get('offset'):
            self.offset = int(data['offset'])
            self.fp.seek(self.offset)
        return ''

    def use(self, number):
        self.number = int(number)
        filename = os.path.join(self.server.path, self.topic, str(self.number))
        if self.fp:
            self.fp.close()
        self.fp = open(filename)
        self.blocksize = os.path.getsize(filename)
        self.offset = 0

    def handle_write(self):
        from sendfile import sendfile
        self.offset += sendfile(self.socket.fileno(), self.fp.fileno(), self.offset, self.blocksize)

    def writable(self):
        if not self.topic:
            return False
        if self.fp and self.blocksize > self.offset:
            return True
        if self.fp:
            self.blocksize = endpoint(self.fp)
            if self.blocksize > self.offset:
                return True

        pathname = os.path.join(self.server.path, str(self.topic))
        if os.path.exists(pathname):
            nums = sorted([int(_) for _ in os.listdir(pathname) if int(_) > self.number])
        else:
            nums = []

        if nums:
            self.use(nums[0])
            return True
        if self.topic in self.server.data and self.server.data[self.topic]:
            self.server.location(self.topic)
        return False

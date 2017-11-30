#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import time
import glob
import logging

from executor import Executor
from event import Event, is_event
from utils import ifilter, endpoint

__author__ = 'tong'

logger = logging.getLogger('stream.logger')


def sleep(delta):
    time.sleep(0 if delta <= 0 else delta)


class Tail(Executor):
    def __init__(self, path, wait=1, times=3, startline=None, position=None, **kwargs):
        super(Tail, self).__init__(**kwargs)
        path = os.path.abspath(path)
        if not os.path.exists(path):
            raise Exception('logsource init failed, cause: No such file %s' % path)
        self.process = None
        self.path = path
        self.stream = None
        self.pos = 0
        self.count = 0
        self.lineno = 0
        self.wait = wait
        self.times = times
        self.startline = startline or 0
        self.position = position or 0

    def open(self, filename):
        if self.stream and not self.stream.closed:
            self.stream.close()
        self.stream = open(filename)
        self.pos = self.stream.tell()
        self.lineno = 0

    def seek(self):
        if self.position > 0:
            self.stream.seek(self.position)
            return
        if self.position < 0:
            self.stream.seek(self.position, 2)
            return
        for i in range(self.startline):
            self.lineno += 1
            self.stream.readline()
        if self.startline < 0:
            self.stream.seek(0, 2)

    def __iter__(self):
        import psutil
        self.process = psutil.Process()
        for event in self.catch():
            yield event
        self.seek()
        while True:
            self.pos = self.stream.tell()
            line = self.stream.readline()
            if not line:
                self.count += 1
                if self.count > self.times:
                    for event in self.redirect():
                        yield event
                    continue
                timer = time.time()
                yield Event.IDLE
                sleep(self.wait - (time.time() - timer))
                self.stream.seek(self.pos)
            else:
                self.count = 0
                self.lineno += 1
                yield line

    def redirect(self):
        self.count = 0
        files = self.process.open_files()
        for fo in files:
            if fo.fd == self.stream.fileno():
                if fo.path == self.path:
                    return
                if not os.path.exists(self.path):
                    logger.info('SOURCE LOG %s does not exist, become: %s' %
                                (self.path, fo.path))
                    return
                logger.info('SOURCE LOG redirect to %s, archive to %s (line count: %s)' %
                            (self.path, fo.path, self.lineno))
                self.open(self.path)
                return
        logger.error('SOURCE LOG fd: %s does not exist? what happened?'
                     % self.stream.fileno())
        for event in self.catch():
            yield event

    def catch(self):
        logger.info('SOURCE LOG try to catch %s...' % self.path)
        while True:
            if os.path.exists(self.path):
                self.open(self.path)
                logger.info('SOURCE LOG catch %s successful' % self.path)
                break
            timer = time.time()
            yield Event.IDLE
            sleep(self.wait - (time.time() - timer))


class File(Executor):
    def __init__(self, path, filewait=None, confirmwait=None, cachefile=None,
                 position=None, startline=None, **kwargs):
        super(File, self).__init__(**kwargs)
        self.path = os.path.abspath(path)
        self.filename = None
        self.file_wait = filewait
        self.confirm_wait = confirmwait
        self.lineno = 0
        self.filter = ifilter('bloom', cachefile)
        self.position = position or 0
        self.startline = startline or 0
        self.stream = None
        logger.info('SOURCE FILE FILTER: %s: %s' % (self.path, cachefile))

    def seek(self):
        if self.position > 0:
            self.stream.seek(self.position)
            return
        if self.position < 0:
            self.stream.seek(self.position, 2)
            return
        for i in range(self.startline):
            self.lineno += 1
            self.stream.readline()
        if self.startline < 0:
            self.stream.seek(0, 2)

    def open(self, filename):
        self.filename = filename
        logger.info('SOURCE FILE dumping %s' % filename)
        try:
            self.stream = open(filename)
            return self.stream
        except Exception, e:
            logger.error('SOURCE FILE open %s failed, cause: %s' % (filename, e))
        return None

    def fetch(self, fp):
        endpos = -1
        self.lineno = 0

        while True:
            pos = fp.tell()
            fp.seek(pos)
            for line in fp:
                if line[-1] == '\n':
                    self.lineno += 1
                    yield line
                else:
                    endpos = fp.tell()
                    fp.seek(-len(line), 1)
                    break
            if not self.confirm_wait:
                break
            else:
                timer = time.time()
                yield Event.IDLE
                sleep(self.confirm_wait - (time.time() - timer))
            ep = endpoint(fp)
            if ep <= fp.tell() or ep <= endpos:
                break
        fp.close()

    def __iter__(self):
        while True:
            files = [_ for _ in glob.glob(self.path)
                     if _ not in self.filter]

            if not files:
                if not self.file_wait:
                    break
                else:
                    timer = time.time()
                    yield Event.IDLE
                    sleep(self.file_wait - (time.time() - timer))
                continue

            logger.info('SOURCE FILE new file %s' % files)
            for filename in sorted(files):
                self.open(filename)
                self.seek()
                if not self.stream:
                    continue

                try:
                    for line in self.fetch(self.stream):
                        yield line
                except Exception, e:
                    logger.error('SOURCE FILE dumping %s failed, cause: %s' % (filename, e))
                    try:
                        self.stream.close()
                        self.stream = None
                    except:
                        pass
                    continue

                if self.stream and not self.stream.closed:
                    self.stream.close()
                self.filter.add(filename)
                logger.info('SOURCE FILE dumping %s End %s' % (filename, self.lineno))


class Csv(File):
    def __init__(self, path, filewait=None, confirmwait=None, cachefile=None, name=None, ignore_exc=True, **kwargs):
        super(Csv, self).__init__(path, filewait, confirmwait, cachefile,
                                  name=name, ignore_exc=ignore_exc, **kwargs)

    def fetch(self, fp):
        reader = csv.reader(fp, **self.kwargs)
        for line in reader:
            self.lineno += 1
            yield line


class Socket(Executor):
    def __init__(self, address, **kwargs):
        super(Socket, self).__init__(**kwargs)
        self.address = address

    def initialize(self):
        import socket
        socket_af = socket.AF_UNIX if isinstance(self.address, basestring) else socket.AF_INET
        sock = socket.socket(socket_af, socket.SOCK_STREAM)
        sock.connect(self.address)
        sock.setblocking(False)
        return sock

    def read(self, sock):
        import socket
        try:
            msg = sock.recv(1024)
            if not msg:
                return None
            return msg
        except socket.error, e:
            if e.errno == 35:
                return Event.IDLE
            else:
                raise Exception('socket %s(%s) error' % (self.address, sock.fileno()))

    def handle(self, message):
        if not message:
            return '', []
        if '\n' not in message:
            data = ''
        else:
            data, message = message.rsplit('\n', 1)
        return message, data.split('\n') if data else []

    def __iter__(self):
        sock = self.initialize()
        message = ''
        while True:
            data = self.read(sock)
            if data is None:
                break
            if is_event(data):
                yield data
                if not message:
                    time.sleep(1)
                    continue
            else:
                message += data
            message, items = self.handle(message)
            for item in items:
                yield item


class Kafka(Executor):
    def __init__(self, topic, servers, EOF=None, **kwargs):
        super(Kafka, self).__init__(**kwargs)
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise Exception('Lack of kafka module, try to execute `pip install kafka-python>=1.3.1` install it')
        self.consumer = KafkaConsumer(topic, bootstrap_servers=servers, **kwargs)
        self.topic = topic
        self.servers = servers
        self.EOF = EOF

    def __iter__(self):
        while True:
            try:
                for msg in self.consumer:
                    if msg.value == self.EOF:
                        break
                    yield msg.value
            except Exception, e:
                logger.error('SOURCE KAFKA fetch failed: topic: %s, '
                             'err: %s' % (self.topic, e))


class Memory(Executor):
    def __init__(self, data, **kwargs):
        super(Memory, self).__init__(**kwargs)
        self.data = data

    def __iter__(self):
        for item in self.data:
            yield item


class Faker(Executor):
    def __init__(self, function, maxsize=None, **kwargs):
        super(Faker, self).__init__(**kwargs)
        self.func = function
        self.maxsize = maxsize

    def __iter__(self):
        i = 0
        while not self.maxsize or i < self.maxsize:
            yield self.func()
            i += 1


class Queue(Executor):
    def __init__(self, queue, EOF=None, wait=None, **kwargs):
        super(Queue, self).__init__(**kwargs)
        self.queue = queue
        self.EOF = EOF
        self.wait = wait or 0

    def __iter__(self):
        from Queue import Empty
        while True:
            try:
                item = self.queue.get_nowait()
                if item == self.EOF:
                    break
                yield item
            except Empty:
                timer = time.time()
                yield Event.IDLE
                sleep(self.wait - (time.time() - timer))


class SQL(Executor):
    def __init__(self, conn, sql, batch=10000, **kwargs):
        super(SQL, self).__init__(**kwargs)
        self.conn = conn
        self.sql = sql
        self.batch = batch

    def __iter__(self):
        cursor = self.conn.execute(self.sql)
        items = cursor.fetchmany(self.batch)
        if items:
            for item in items:
                yield item

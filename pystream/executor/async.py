#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from asyncore import dispatcher

from .event import is_event

__author__ = 'tong'

logger = logging.getLogger('stream.logger')


class TCPClient(dispatcher):
    def __init__(self, address):
        import socket
        dispatcher.__init__(self)
        socket_af = socket.AF_UNIX if isinstance(address, basestring) else socket.AF_INET
        self.create_socket(socket_af, socket.SOCK_STREAM)
        self.connect(address)
        self.message = None
        self.iterator = None
        self._source = None

    def handle_connect(self):
        pass

    def handle_read(self):
        pass

    def handle_error(self):
        logger.error('client[TCP] socket %s error' % str(self.addr))
        self.handle_close()

    def handle_expt(self):
        logger.error('client[TCP] socket %s error: unhandled incoming priority event' % self.addr)

    def handle_close(self):
        logger.info('client[TCP] socket %s close' % str(self.addr))
        self.close()

    def writable(self):
        self.message = next(self.iterator)
        return not is_event(self.message)

    def handle_write(self):
        sent = self.send(self.message+'\n')
        logger.debug('OUTPUT socket a message(%s)' % sent)

    def start(self):
        import asyncore
        self.iterator = iter(self.source)
        asyncore.loop(use_poll=True)

    @property
    def source(self):
        if not self._source:
            raise Exception('Lack of data source!')
        return self._source

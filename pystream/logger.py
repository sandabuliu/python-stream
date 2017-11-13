#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import time
import logging
from random import randint
from traceback import format_exc

__author__ = 'tong'

logger = logging.getLogger('stream.logger')
tracer = logging.getLogger('stream.tracer')


class LogTracer(logging.Filter):
    def filter(self, record):
        if record.levelno >= logging.WARN:
            extra = getattr(record, 'extra', {})
            trace_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, '%s%s' % (time.time(), randint(0, 100000))))
            record.trace_id = trace_id
            msg = '\n'.join(['[%s]\n%s' % (k.strip(), str(v).strip()) for k, v in extra.items()])
            tracer.error('[%s]: %s[format_exc]\n%s' % (trace_id, msg and ('\n%s\n' % msg), format_exc()))
        return True

logger.addFilter(LogTracer())

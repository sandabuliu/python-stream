#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from datetime import datetime, date, time

__author__ = 'tong'


class JSONCls(json.JSONEncoder):
    def default(self, obj):
        from sqlalchemy.engine.result import RowProxy
        if isinstance(obj, RowProxy):
            return list(obj)
        if isinstance(obj, datetime):
            return '%04d-%02d-%02d %02d:%02d:%02d' % (obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second)
        if isinstance(obj, date):
            return '%04d-%02d-%02d' % (obj.year, obj.month, obj.day)
        if isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        return json.JSONEncoder.default(self, obj)


class DefaultParser(object):
    def parse(self, data):
        from .logparser.logparser import ParserResult
        return ParserResult(data, data, data)

    @property
    def rule(self):
        return None

    @property
    def fieldnames(self):
        return []

    @property
    def fieldtypes(self):
        return {}

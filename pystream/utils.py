#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from datetime import datetime, date, time

__author__ = 'tong'


class JSONCls(json.JSONEncoder):
    def __init__(self, skipkeys=False, ensure_ascii=False, check_circular=True, allow_nan=True, sort_keys=False,
                 indent=None, separators=None, encoding='', default=None):
        def set_default(obj):
            from sqlalchemy.engine.result import RowProxy
            if isinstance(obj, RowProxy):
                return list(obj)
            raise TypeError('%s(%s) is not JSON serializable' % (obj, type(obj)))

        super(JSONCls, self).__init__(skipkeys, ensure_ascii, check_circular, allow_nan, sort_keys, indent, separators,
                                      encoding, default or set_default)

    def default(self, obj):
        if isinstance(obj, datetime):
            return '%04d-%02d-%02d %02d:%02d:%02d' % (obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second)
        if isinstance(obj, date):
            return '%04d-%02d-%02d' % (obj.year, obj.month, obj.day)
        if isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        else:
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

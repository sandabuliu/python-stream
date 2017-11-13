#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys
import json
import csv
import urllib
from pyparsing import nestedExpr, OneOrMore, CharsNotIn, Or, Empty
from datatype import Datatype

__author__ = 'tong'

reload(sys)
sys.setdefaultencoding("utf-8")


class Rulebase(object):
    TYPE = basestring
    SINGLE_RET = False


class Regex(Rulebase):
    def __init__(self, rule):
        self.reg = rule
        self.rule = re.compile(rule)

    def parse(self, log):
        res = re.match(self.rule, log)
        ret = res.groupdict()
        if not ret:
            ret = {str(i): value for i, value in enumerate(res.groups())}
        return ret


class Form(Rulebase):
    TYPE = dict

    def __init__(self, rule):
        self.rule = rule
        self.opener = self.rule['opener']
        self.closer = self.rule['closer']
        self.columns = self.rule.get('columns', -1)

        nested = nestedExpr(opener=self.opener, closer=self.closer,
                            content=CharsNotIn(self.opener + self.closer))
        if self.columns < 0:
            self.nested = OneOrMore(nested)
        else:
            self.nested = nested * self.columns + Or([CharsNotIn('\n'), Empty()])

    def merge(self, items):
        return '%s%s%s' % (self.opener,
                           ''.join([self.merge(item)
                                    if isinstance(item, list)
                                    else str(item)
                                    for item in items]),
                           self.closer)

    def parse(self, log):
        ret = self.nested.parseString(log).asList()
        start = len(self.opener)
        end = -len(self.closer)
        return {str(i): self.merge(value)[start:end].strip()
                for i, value in enumerate(ret)}


class Split(Rulebase):
    TYPE = dict

    def __init__(self, rule):
        self.rule = rule
        self.separator = self.rule['separator']
        self.maxsplit = self.rule.get('maxsplit', -1)

    def parse(self, log):
        ret = log.split(self.separator, self.maxsplit)
        return {str(i): value.strip() for i, value in enumerate(ret)}


class Type(Rulebase):
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = Datatype.get(rule)

    def parse(self, log):
        return {'0': self.rule(log).data}


class Kv(Rulebase):
    TYPE = dict

    def __init__(self, rule):
        self.rule = rule
        self.separator = self.rule['separator']
        self.linker = self.rule.get('linker', '=')
        self.pattern = re.compile(r'^[a-zA-Z_]\w*$')
        self.not_check = not self.rule.get('strict', False)

    def parse(self, log):
        ret = log.split(self.separator)
        ret = [value.split(self.linker, 1) for value in ret if self.linker in value]
        return {key.strip(): value.strip() for key, value in ret
                if self.not_check or re.match(self.pattern, key.strip())}


class Macro(Rulebase):
    TYPE = dict
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule

    def parse(self, log):
        return {'0': self.rule.get(log, log)}


class Csv(Rulebase):
    TYPE = dict

    def __init__(self, rule):
        self.data = Csv.Iterator()
        self.rule = rule
        self.csv = csv.reader(self.data, **rule)

    def parse(self, log):
        self.data.append(log)
        ret = self.csv.next()
        return {str(i): item for i, item in enumerate(ret)}

    class Iterator(object):
        def __init__(self):
            self.data = ''

        def append(self, data):
            self.data = data

        def __iter__(self):
            return self

        def next(self):
            data = self.data
            if data is None:
                raise StopIteration()
            self.data = None
            return data


class Encode(Rulebase):
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule

    def parse(self, log):
        if self.rule == 'urlquote':
            return {'0': urllib.quote(log)}
        if self.rule == 'urlquote_plus':
            return {'0': urllib.quote_plus(log)}
        return {'0': log.encode(self.rule)}


class Decode(Rulebase):
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule

    def parse(self, log):
        if self.rule == 'urlquote':
            return {'0': urllib.unquote(log)}
        if self.rule == 'urlquote_plus':
            return {'0': urllib.unquote_plus(log)}
        return {'0': log.decode(self.rule)}


class Json(Rulebase):
    TYPE = bool

    def __init__(self, rule):
        self.rule = rule

    def parse(self, log):
        ret = json.loads(log)
        if self.rule:
            for key, value in ret.items():
                if isinstance(value, (list, dict)):
                    ret[key] = json.dumps(value)
        return ret


class Endswith(Rulebase):
    TYPE = dict
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule
        self.suffix = self.rule['suffix']
        self.start = self.rule.get('start')
        self.end = self.rule.get('end')

    def parse(self, log):
        ret = log.endswith(self.suffix, self.start, self.end)
        return {str(ret): log}


class Startswith(Rulebase):
    TYPE = dict
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule
        self.suffix = self.rule['suffix']
        self.start = self.rule.get('start')
        self.end = self.rule.get('end')

    def parse(self, log):
        ret = log.startswith(self.suffix, self.start, self.end)
        return {str(ret): log}


class Contain(Rulebase):
    TYPE = dict
    SINGLE_RET = True

    def __init__(self, rule):
        self.rule = rule
        self.suffix = self.rule['suffix']
        self.start = self.rule.get('start')
        self.end = self.rule.get('end')

    def parse(self, log):
        ret = self.suffix in log[self.start: self.end]
        return {str(ret): log}

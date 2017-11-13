#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from utils import JSONCls

__author__ = 'tong'


class Expr(object):
    def __init__(self, name=None):
        self.name = name or '_'
        self.operator = lambda x, y: x == y
        self.value = None
        self.linker = '='

    def set(self, o, v, l):
        self.operator = o
        self.value = v
        self.linker = l
        return self

    def data(self, data):
        pass

    def result(self, data):
        return self.operator(self.data(data), self.value)

    def __eq__(self, other):
        return self.set(lambda x, y: x == y, other, '==')

    def __ne__(self, other):
        return self.set(lambda x, y: x != y, other, '!=')

    def __lt__(self, other):
        return self.set(lambda x, y: x < y, other, '<')

    def __gt__(self, other):
        return self.set(lambda x, y: x > y, other, '>')

    def __le__(self, other):
        return self.set(lambda x, y: x <= y, other, '<=')

    def __ge__(self, other):
        return self.set(lambda x, y: x >= y, other, '>=')

    def contain(self, other):
        return self.set(lambda x, y: y in x, other, 'in')

    def In(self, other):
        return self.set(lambda x, y: x in y, other, 'in')

    def __or__(self, other):
        return Or(self, other)

    def __and__(self, other):
        return And(self, other)

    def __str__(self):
        return '`%s` %s %s' % (self.name, self.linker, json.dumps(self.value, cls=JSONCls))


class Text(Expr):
    def data(self, data):
        return data


class Key(Expr):
    def data(self, data):
        if not isinstance(data, dict):
            raise Exception('data should be a dict')
        return data.get(self.name)


class And(object):
    def __init__(self, *args):
        self.args = args

    def result(self, data):
        return all([v.result(data) for v in self.args])

    def __str__(self):
        return ' And '.join(['(%s)' % i for i in self.args])


class Or(object):
    def __init__(self, *args):
        self.args = args

    def result(self, data):
        return any([v.result(data) for v in self.args])

    def __str__(self):
        return ' Or '.join(['(%s)' % i for i in self.args])

_ = Text()

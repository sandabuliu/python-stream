#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from . import Rule
from exception import RuleException

__author__ = 'tong'


class ParserResult(object):
    def __init__(self, line, trace, result):
        self._line = line
        self._trace = trace
        self._result = result

    def line(self):
        return self._line

    def trace(self):
        return self._trace

    def result(self):
        return self._result


class LogParser(object):
    def __init__(self, rule):
        self._rule = Rule(rule)
        self._fields = rule.get('fields', {})
        self._parser = {}

        for name, subrule in self._rule.subrules.iteritems():
            self._parser[name] = LogParser(subrule)

    @property
    def rule(self):
        return self._rule

    @property
    def fieldnames(self):
        ret = self._fields.keys()
        for name, parser in self._parser.items():
            ret += parser.fieldnames
        return list(set(ret))

    @property
    def fieldtypes(self):
        keys = self._fields.keys()
        datatype = 'string'
        if self.rule.type == 'Type':
            datatype = self.rule.rule.lower()

        ret = {}.fromkeys(keys, datatype)
        for name, parser in self._parser.items():
            ret.update(parser.fieldtypes)
        return ret

    def parse(self, log):
        ret = self._rule.parse(log.strip('\n'))
        if not ret:
            return ParserResult(log, {}, {})

        trace = ret
        result = {k: ret[v] for k, v in self._fields.iteritems() if v in ret}
        for key in self._rule.subrules:
            if key in ret:
                res = self._parser[key].parse(ret[key])
                trace[key] = res.trace()
                result.update(res.result())
        return ParserResult(log, self.trace(trace), result)

    def trace(self, trace):
        if not trace:
            return {}
        if self.rule.ruleparser.SINGLE_RET:
            return trace[trace.keys()[0]]
        return trace


class RuleEditor(object):
    def __init__(self, text=''):
        self._text = text
        self._name = None
        self._rule = None
        self._subrules = {}
        self._results = {}

    def __str__(self):
        if self._results:
            ret = {subrule(): key for key, subrule in self._subrules.items() if subrule()}
            return json.dumps(self._results, indent=2)+'\n'+json.dumps(ret, indent=2)
        return self._text

    __repr__ = __str__

    def __call__(self, name=None):
        if name:
            self._name = name
        return self._name

    def __getattr__(self, key):
        try:
            if key not in self._subrules:
                self._subrules[key] = RuleEditor(self._results.get(key))
            return self._subrules[key]
        except Exception, e:
            raise RuleException('key must in %s [%s: %s]' %
                                (self._results.keys(), key, e))

    __getitem__ = __getattr__

    def load(self, rule):
        self._subrules = {}
        self.parse(rule['type'], rule['rule'])
        for key, value in rule.get('fields', {}).items():
            self.__getattr__(value)(key)
        for key, subrule in rule.get('subrules', {}).items():
            self.__getattr__(key).load(subrule)

    def parse(self, type, rule=None, **kwargs):
        self._rule = Rule()
        self._rule.type = type
        self._rule.rule = rule or json.dumps(kwargs)

        self._subrules = {}
        if self._text is not None:
            self._results = self._rule.parse(self._text)
        return self._results

    def rule(self):
        if self._rule is None:
            return None

        ret = self._rule()
        ret['fields'] = {}
        for key, subrule in self._subrules.items():
            r = subrule.rule()
            if r:
                ret['subrules'][key] = r

            if subrule():
                ret['fields'][subrule()] = key
        return ret

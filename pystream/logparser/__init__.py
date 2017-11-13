#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ruleparser
from exception import RuleException, ParseException

__author__ = 'tong'


class Rule(object):
    def __init__(self, rule=None):
        self._type = None
        self._rule = None
        self._subrules = {}
        self._parser = None
        self._ruleparser = None
        rule = rule or {}
        if not isinstance(rule, dict):
            raise RuleException('Input rule should be dict!')

        if rule:
            self.type = rule.get('type')
            self.rule = rule.get('rule')
            self.subrules = rule.get('subrules', {})

    def __str__(self):
        return '[TYPE] %s [RULE] %s [SUBRULES] %s' % (self.type, self.rule, self.subrules)

    def parse(self, text):
        parser = self.parser
        try:
            return parser.parse(text)
        except Exception, e:
            raise ParseException(e, text, self.type, self.rule)

    def __call__(self):
        return {
            'type': self.type.lower(),
            'rule': self.rule,
            'subrules': self.subrules
        }

    @property
    def parser(self):
        if not self._parser:
            try:
                self._parser = self._ruleparser(self.rule)
            except Exception, e:
                raise RuleException('%s rule error: %s (%s)' % (self.type, self.rule, e))
        return self._parser

    @property
    def type(self):
        if self._type is None:
            raise RuleException('Lack of type')
        return self._type

    @type.setter
    def type(self, value):
        value = str(value).lower().capitalize()
        if not hasattr(ruleparser, value):
            raise RuleException('Unsupported rule (%s)!' % value)
        self._type = value
        self._parser = None
        self._ruleparser = getattr(ruleparser, self.type)

    @property
    def ruleparser(self):
        if not self._ruleparser:
            raise RuleException('Type not setting')
        return self._ruleparser

    @property
    def rule(self):
        if isinstance(self._rule, self.ruleparser.TYPE):
            return self._rule
        raise RuleException('Lack of rule')

    @rule.setter
    def rule(self, value):
        rule_type = self.ruleparser.TYPE
        if not isinstance(value, rule_type):
            raise RuleException('The context of rule (%s) must be %s ! (%s: %s)'
                                % (self.type, rule_type, type(value), value))
        self._rule = value
        self._parser = None

    @property
    def subrules(self):
        return self._subrules

    @subrules.setter
    def subrules(self, value):
        if not isinstance(value, dict):
            raise RuleException('Sub rule should be dict!')
        self._subrules = value

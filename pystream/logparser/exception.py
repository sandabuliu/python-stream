#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


class LogParserException(Exception):
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return '%s: %s' % (self.__class__.__name__, self.message)


class RuleException(LogParserException):
    pass


class ParseException(LogParserException):
    def __init__(self, msg, line=None, type=None, rule=None):
        super(ParseException, self).__init__(msg)
        self.line = line
        self.type = type
        self.rule = rule


class ClassException(LogParserException):
    pass
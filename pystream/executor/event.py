#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


class Enum(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __eq__(self, other):
        return self.value == other.value

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<pystream Event: %s>' % self.name


class Event(object):
    SKIP = Enum('SKIP', 1)
    IDLE = Enum('IDLE', 2)


def is_event(obj):
    return isinstance(obj, Enum)

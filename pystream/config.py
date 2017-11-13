#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import pytz
import ConfigParser
from datetime import datetime

from logic import Key, And, Or, Text, _

__author__ = 'tong'


ROOT_PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))

PATH = {
    'ROOT': ROOT_PATH,
    'RULE': [os.path.join(ROOT_PATH, 'rule')]
}


def config(filename):
    if not os.path.exists(filename):
        raise Exception('No such config file %s' % filename)
    cfg = ConfigParser.ConfigParser()
    cfg.read(filename)
    return cfg


def parse(value):
    class FieldDict(object):
        def __init__(self, e):
            self.e = e

        def __getitem__(self, item):
            return self.e.get(item, Key(item))

    env = {
        'Text': Text,
        'Key': Key,
        'And': And,
        'Or': Or,
        'pytz': pytz,
        'datetime': datetime,
        '_': _
    }
    result = eval(value, env, FieldDict(env))
    if isinstance(result, (tuple, list)):
        result = And(*result)
    return result


def rule(name='root', rulebase=None):
    def _decode_list(data):
        rv = []
        for item in data:
            if isinstance(item, unicode):
                item = item.encode('utf-8')
            elif isinstance(item, list):
                item = _decode_list(item)
            elif isinstance(item, dict):
                item = _decode_dict(item)
            rv.append(item)
        return rv

    def _decode_dict(data):
        rv = {}
        for key, value in data.iteritems():
            if isinstance(key, unicode):
                key = key.encode('utf-8')
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            elif isinstance(value, list):
                value = _decode_list(value)
            elif isinstance(value, dict):
                value = _decode_dict(value)
            rv[key] = value
        return rv

    bools = {'1': True, 'yes': True, 'true': True, 'on': True,
             '0': False, 'no': False, 'false': False, 'off': False}

    def rule_type(rl):
        from logparser import Rule
        r = Rule()
        r.type = rl['type']
        value = rl.get('rule')
        if r.ruleparser.TYPE is dict:
            value = json.loads(value, object_hook=_decode_dict)
        if r.ruleparser.TYPE is bool:
            value = bools.get(value.lower(), False)
        return value
    for rulebase in ([rulebase] if rulebase else PATH['RULE']):
        try:
            rules = dict(config(rulebase).items(name))
            break
        except Exception:
            pass
    else:
        raise Exception('No such rule %s' % name)

    ret = {'type': rules['type'], 'rule': rule_type(rules)}
    if rules.get('fields'):
        try:
            fields = json.loads(rules['fields'], object_hook=_decode_dict)
        except:
            fields = [i.strip() for i in rules['fields'].split(',')]
        if isinstance(fields, list):
            fields = dict(zip(fields, [str(i) for i in range(len(fields))]))
        fields.pop(None, None)
        fields.pop('', None)
        ret['fields'] = fields
    if rules.get('subrules'):
        ret['subrules'] = {k: rule(v, rulebase) for k, v in
                           json.loads(rules['subrules'], object_hook=_decode_dict).items()}
    return ret

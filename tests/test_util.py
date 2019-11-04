import pytest
from collections import OrderedDict
from pyracmon.util import *


class TestSplitDict:
    def test_split(self):
        k, v = split_dict(dict(a = 1, b = 2, c = 3))
        assert k == ['a', 'b', 'c']
        assert v == [1, 2, 3]


class TestIndexQualifier:
    def test_int_keys(self):
        r = index_qualifier({ 1: 'a', 2: 'b', 3: 'c'}, [])
        assert r == { 1: 'a', 2: 'b', 3: 'c'}

    def test_string_keys(self):
        r = index_qualifier({ 'a': 1, 'b': 2, 'c': 3}, ['c', 'a', 'b'])
        assert r == { 1: 1, 2: 2, 0: 3 }

    def test_mixed_keys(self):
        r = index_qualifier({ 'a': 1, 4: 2, 'c': 3}, ['c', 'a', 'b'])
        assert r == { 1: 1, 4: 2, 0: 3 }

    def test_unknown_key(self):
        with pytest.raises(ValueError):
            r = index_qualifier({ 'a': 1, 'b': 2, 'c': 3}, ['c', 'a'])

class Model:
    def __init__(self, values):
        self.values = values

    def __iter__(self):
        class C:
            def __init__(self, name):
                self.name = name
        return iter([(C(k), v) for k, v in self.values.items()])


class TestModelValues:
    def test_dict(self):
        d = dict(a = 1, b = 2, c = 3)
        r = model_values(None, d)
        assert r == d

    def test_model(self):
        d = OrderedDict(a = 1, b = 2, c = 3)
        r = model_values(Model, Model(d))
        assert r == d

    def test_otherwise(self):
        with pytest.raises(TypeError):
            d = dict(a = 1, b = 2, c = 3)
            r = model_values(int, Model(d))
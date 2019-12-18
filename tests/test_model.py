import pytest
from pyracmon.model import *


class A:
    @classmethod
    def m1(cls):
        return "A1"
    @classmethod
    def m2(cls):
        return "A2"

class B:
    @classmethod
    def m2(cls):
        return "B2"
    @classmethod
    def m3(cls):
        return "B3"

table1 = Table("t1", [
    Column("c1", int, None, True, False, "seq"),
    Column("c2", int, None, False, False, None),
    Column("c3", int, None, False, False, None),
])

class TestDefineModel:
    def test_define(self):
        m = define_model(table1)
        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]

    def test_mixins(self):
        m = define_model(table1, [A, B])
        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]
        assert m.m1() == "A1"
        assert m.m2() == "A2"
        assert m.m3() == "B3"

class TestModelInstance:
    def test_create(self):
        m = define_model(table1)
        v = m(c1 = 1, c3 = "abc")
        assert v.c1 == 1
        assert v.c3 == "abc"
        assert not hasattr(v, "c2")

    def test_iterate(self):
        m = define_model(table1)
        vs = list(m(c1 = 1, c3 = "abc"))
        assert vs == [(table1.columns[0], 1), (table1.columns[2], "abc")]

table2 = Table("t2", [
    Column("c1", int, None, True, False, "seq"),
    Column("c2", int, None, True, False, None),
    Column("c3", int, None, False, False, None),
])

class TestParsePKs:
    def test_dict(self):
        m = define_model(table1)
        assert m._parse_pks(dict(c1 = 1)) == (["c1"], [1])

    def test_singular(self):
        m = define_model(table1)
        assert m._parse_pks(1) == (["c1"], [1])

    def test_unknown(self):
        with pytest.raises(ValueError):
            m = define_model(table1)
            m._parse_pks(dict(c2 = 1))

    def test_multiple(self):
        m = define_model(table2)
        assert m._parse_pks(dict(c1 = 1, c2 = 3)) == (["c1", "c2"], [1, 3])

    def test_invalid_singular(self):
        with pytest.raises(ValueError):
            m = define_model(table2)
            m._parse_pks(1)
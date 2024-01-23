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


class AB(Model, A, B): pass


table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, False, None, None, False),
    Column("c3", int, None, False, None, None, True),
])
class T1(Meta): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN


table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, None, None, True),
    Column("c3", int, None, False, None, None, False),
])
class T2(Meta): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN


class TestDefineModel:
    def test_define(self):
        m = define_model(table1, model_type=T1)

        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]
        assert m.table == table1
        assert m.columns == table1.columns
        assert m.column.c1 is table1.columns[0]
        assert m.column.c2 is table1.columns[1]
        assert m.column.c3 is table1.columns[2]

        v = m()
        assert not hasattr(v, "name")
        assert not hasattr(v, "c1")

    def test_mixins(self):
        class T1AB(T1, A, B): ...
        m = define_model(table1, Mixins[A, B], model_type=T1AB)

        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]
        assert m.m1() == "A1"
        assert m.m2() == "A2"
        assert m.m3() == "B3"


class TestShrink:
    def test_shrink(self):
        class T1AB(T1, A, B): ...
        m = define_model(table1, Mixins[A, B], model_type=T1AB)

        n = m.shrink(["c2"])

        assert n.name == "t1"
        assert [c.name for c in n.columns] == ["c1", "c3"]
        assert n.m1() == "A1"
        assert n.m2() == "A2"
        assert n.m3() == "B3"

        with pytest.raises(TypeError):
            n(c2=1)

    def test_includes(self):
        class T1AB(T1, A, B): ...
        m = define_model(table1, [A, B], model_type=T1AB)

        n = m.shrink(["c2"], ["c1"])

        assert n.name == "t1"
        assert [c.name for c in n.columns] == ["c1"]
        assert n.m1() == "A1"
        assert n.m2() == "A2"
        assert n.m3() == "B3"

        with pytest.raises(TypeError):
            n(c3=1)


class TestModelInstance:
    def test_create(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)

        assert v.c1 == 1
        assert v.c3 == 3
        assert not hasattr(v, "c2")
        assert list(v) == [(table1.columns[0], 1), (table1.columns[2], 3)]

    def test_get_item(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)

        assert v['c1'] == 1
        assert v['c3'] == 3
        assert 'c2' not in v

    def test_set(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)

        v.c1 = 2
        v.c2 = 4

        assert v.c1 == 2
        assert v.c2 == 4
        assert v.c3 == 3
        assert list(v) == [(table1.columns[0], 2), (table1.columns[1], 4), (table1.columns[2], 3)]

    def test_create_error(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(TypeError):
            v = m(c1 = 1, c3 = "abc", c4 = 2) # type: ignore

    def test_set_error(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)
        with pytest.raises(TypeError):
            v.c4 = 2 # type: ignore

    def test_repr(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)

        assert repr(v) == "t1(c1=1, c3=3)"

    def test_str(self):
        m = define_model(table1, model_type=T1)
        v = m(c1 = 1, c3 = 3)

        assert repr(v) == "t1(c1=1, c3=3)"


class TestEqual:
    def test_equal(self):
        m = define_model(table1, model_type=T1)
        assert m(c1 = 1, c2 = 2, c3 = 3) == m(c1 = 1, c2 = 2, c3 = 3)

    def test_subset(self):
        m = define_model(table1, model_type=T1)
        assert m(c1 = 1, c3 = 3) == m(c1 = 1, c3 = 3)

    def test_different(self):
        m = define_model(table1, model_type=T1)
        assert m(c1 = 1, c2 = 2, c3 = 3) != m(c1 = 1, c2 = 5, c3 = 3)

    def test_different_type(self):
        m1 = define_model(table1, model_type=T1)
        m2 = define_model(table2, model_type=T2)
        assert m1(c1 = 1, c2 = 2, c3 = 3) != m2(c1 = 1, c2 = 2, c3 = 3)

    def test_shortage(self):
        m = define_model(table1, model_type=T1)
        assert m(c1 = 1, c2 = None, c3 = 3) != m(c1 = 1, c3 = 3) # type: ignore

    def test_redundant(self):
        m = define_model(table1, model_type=T1)
        assert m(c1 = 1, c3 = 3) != m(c1 = 1, c2 = None, c3 = 3) # type: ignore


class TestParsePKs:
    def test_dict(self):
        m = define_model(table1, model_type=T1)
        assert parse_pks(m, dict(c1 = 1)) == (["c1"], [1])

    def test_singular(self):
        m = define_model(table1, model_type=T1)
        assert parse_pks(m, 1) == (["c1"], [1])

    def test_unknown(self):
        with pytest.raises(ValueError):
            m = define_model(table1, model_type=T1)
            parse_pks(m, dict(c2 = 1))

    def test_multiple(self):
        m = define_model(table2, model_type=T2)
        assert parse_pks(m, dict(c1 = 1, c2 = 3)) == (["c1", "c2"], [1, 3])

    def test_invalid_singular(self):
        with pytest.raises(ValueError):
            m = define_model(table2, model_type=T2)
            parse_pks(m, 1)


class TestCheckColumns:
    def test_check(self):
        m = define_model(table1, model_type=T1)
        check_columns(m, dict(c1 = 1, c2 = 2, c3 = 3))

    def test_condition_ok(self):
        m = define_model(table1, model_type=T1)
        check_columns(m, dict(c2 = 2, c3 = 3), lambda c: not c.pk)

    def test_condition_ng(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c1 = 1, c2 = 2, c3 = 3), lambda c: not c.pk)

    def test_not_all(self):
        m = define_model(table1, model_type=T1)
        check_columns(m, dict(c2 = 2, c3 = 3))

    def test_requires_all(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c2 = 2, c3 = 3), requires_all=True)

    def test_requires_condition(self):
        m = define_model(table1, model_type=T1)
        check_columns(m, dict(c2 = 2, c3 = 3), lambda c: not c.pk, requires_all=True)

    def test_unknown(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c1 = 1, c3 = 3, c4 = 4))


class TestModelValues:
    def test_dict(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1 = 1, c2 = 2, c3 = 3))
        assert values == dict(c1 = 1, c2 = 2, c3 = 3)

    def test_dict_redundant(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1 = 1, c2 = 2, c3 = 3, c4 = 4))
        assert values == dict(c1 = 1, c2 = 2, c3 = 3)

    def test_dict_excludes_pk(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1 = 1, c2 = 2, c3 = 3), excludes_pk=True)
        assert values == dict(c2 = 2, c3 = 3)

    def test_model(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, m(c1 = 1, c2 = 2, c3 = 3))
        assert values == dict(c1 = 1, c2 = 2, c3 = 3)

    def test_model_excludes_pk(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, m(c1 = 1, c2 = 2, c3 = 3), excludes_pk=True)
        assert values == dict(c2 = 2, c3 = 3)

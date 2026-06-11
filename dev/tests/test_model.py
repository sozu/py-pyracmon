import pytest
from pyracmon.model import Mixins, define_model, parse_pks, extract_pks, check_columns, model_values
from .fixtures import *


class T1(Model): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN
class T2(Model): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN


class TestDefineModel:
    def test_define(self):
        m = define_model(table1, model_type=T1)

        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]
        assert m.table == table1
        assert list(m.columns) == table1.columns
        assert m.columns.c1 is table1.columns[0]
        assert m.columns.c2 is table1.columns[1]
        assert m.columns.c3 is table1.columns[2]

        v = m()
        assert not hasattr(v, "name")
        assert not hasattr(v, "c1")

    def test_mixins(self):
        class A:
            @classmethod
            def m1(cls): return "A1"
            @classmethod
            def m2(cls): return "A2"

        class B:
            @classmethod
            def m2(cls): return "B2"
            @classmethod
            def m3(cls): return "B3"

        class T1AB(T1, A, B):
            ...

        m = define_model(table1, Mixins[A, B], model_type=T1AB)

        assert m.name == "t1"
        assert [c.name for c in m.columns] == ["c1", "c2", "c3"]
        assert m.m1() == "A1"
        assert m.m2() == "A2"
        assert m.m3() == "B3"


class TestModelInstance:
    def test_attr(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)

        assert v.c1 == 1
        assert v.c3 == 3
        assert not hasattr(v, "c2")
        assert list(v) == [(table1.columns[0], 1), (table1.columns[2], 3)]

    def test_item(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)

        assert v['c1'] == 1
        assert v['c3'] == 3
        assert v[table1.columns[0]] == 1
        assert v[table1.columns[2]] == 3
        assert 'c1' in v
        assert 'c2' not in v
        assert 'c3' in v
        assert table1.columns[0] in v
        assert table1.columns[1] not in v
        assert table1.columns[2] in v

    def test_set(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)

        v.c1 = 2
        v.c2 = 4

        assert v.c1 == 2
        assert v.c2 == 4
        assert v.c3 == 3
        assert list(v) == [(table1.columns[0], 2),
                           (table1.columns[1], 4), (table1.columns[2], 3)]

    def test_invalid_column_create(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(TypeError):
            v = m(c1=1, c3="abc", c4=2)  # type: ignore

    def test_invalid_column_set(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)
        with pytest.raises(TypeError):
            v.c4 = 2  # type: ignore

    def test_repr(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)
        assert repr(v) == "t1(c1=1, c3=3)"

    def test_str(self):
        m = define_model(table1, model_type=T1)
        v = m(c1=1, c3=3)
        assert str(v) == "t1(c1=1, c3=3)"


class TestEqual:
    def test_equal(self):
        m = define_model(table1, model_type=T1)
        assert m(c1=1, c2=2, c3=3) == m(c1=1, c2=2, c3=3)

    def test_subset(self):
        m = define_model(table1, model_type=T1)
        assert m(c1=1, c3=3) == m(c1=1, c3=3)

    def test_different(self):
        m = define_model(table1, model_type=T1)
        assert m(c1=1, c2=2, c3=3) != m(c1=1, c2=5, c3=3)

    def test_different_type(self):
        m1 = define_model(table1, model_type=T1)
        m2 = define_model(table2, model_type=T2)
        assert m1(c1=1, c2=2, c3=3) != m2(c1=1, c2=2, c3=3)

    def test_shortage(self):
        m = define_model(table1, model_type=T1)
        assert m(c1=1, c2=None, c3=3) != m(c1=1, c3=3)  # type: ignore

    def test_redundant(self):
        m = define_model(table1, model_type=T1)
        assert m(c1=1, c3=3) != m(c1=1, c2=None, c3=3)  # type: ignore


class TestParsePks:
    def test_dict(self):
        m = define_model(table1, model_type=T1)
        assert parse_pks(m, dict(c1=1)) == (["c1"], [1])

    def test_singular(self):
        m = define_model(table1, model_type=T1)
        assert parse_pks(m, 1) == (["c1"], [1])

    def test_multiple(self):
        m = define_model(table2, model_type=T2)
        assert parse_pks(m, dict(c1=1, c2=3)) == (["c1", "c2"], [1, 3])

    def test_not_pk(self):
        with pytest.raises(ValueError):
            m = define_model(table1, model_type=T1)
            parse_pks(m, dict(c2=1))

    def test_shortage(self):
        with pytest.raises(ValueError):
            m = define_model(table2, model_type=T2)
            parse_pks(m, 1)

    def test_redundant(self):
        with pytest.raises(ValueError):
            m = define_model(table1, model_type=T1)
            parse_pks(m, dict(c1=1, c2=3))


class TestExtractPks:
    def test_model(self):
        m = define_model(table1, model_type=T1)
        assert extract_pks(m, m(c1=1, c2=2, c3=3)) == dict(c1=1)

    def test_dict(self):
        m = define_model(table1, model_type=T1)
        assert extract_pks(m, dict(c1=1, c2=2, c3=3)) == dict(c1=1)

    def test_multi_model(self):
        m = define_model(table2, model_type=T2)
        assert extract_pks(m, m(c1=1, c2=2, c3=3)) == dict(c1=1, c2=2)

    def test_multi_dict(self):
        m = define_model(table2, model_type=T2)
        assert extract_pks(m, dict(c1=1, c2=2, c3=3)) == dict(c1=1, c2=2)

    def test_missing(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            extract_pks(m, dict(c2=2, c3=3))

    def test_missing_multi(self):
        m = define_model(table2, model_type=T2)
        with pytest.raises(ValueError):
            extract_pks(m, dict(c1=1, c3=3))


class TestCheckColumns:
    def test_all(self):
        m = define_model(table1, model_type=T1)
        assert check_columns(m, dict(c1=1, c2=2, c3=3)) == [("c1", 1), ("c2", 2), ("c3", 3)]

    def test_subset(self):
        m = define_model(table1, model_type=T1)
        assert check_columns(m, dict(c2=2, c3=3)) == [("c2", 2), ("c3", 3)]

    def test_requires_all(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c2=2, c3=3), requires_all=True)

    def test_condition(self):
        m = define_model(table1, model_type=T1)
        assert check_columns(m, dict(c2=2, c3=3), lambda c: not c.pk) == [("c2", 2), ("c3", 3)]

    def test_condition_ng(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c1=1, c2=2, c3=3), lambda c: not c.pk)

    def test_requires_condition(self):
        m = define_model(table1, model_type=T1)
        assert check_columns(m, dict(c2=2, c3=3), lambda c: not c.pk, requires_all=True) == [("c2", 2), ("c3", 3)]

    def test_unknown(self):
        m = define_model(table1, model_type=T1)
        with pytest.raises(ValueError):
            check_columns(m, dict(c1=1, c3=3, c4=4))


class TestModelValues:
    def test_dict(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1=1, c2=2, c3=3))
        assert values == dict(c1=1, c2=2, c3=3)

    def test_dict_subset(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1=1, c2=2))
        assert values == dict(c1=1, c2=2)

    def test_dict_redundant(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1=1, c2=2, c3=3, c4=4))
        assert values == dict(c1=1, c2=2, c3=3)

    def test_dict_excludes_pk(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, dict(c1=1, c2=2, c3=3), excludes_pk=True)
        assert values == dict(c2=2, c3=3)

    def test_model(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, m(c1=1, c2=2, c3=3))
        assert values == dict(c1=1, c2=2, c3=3)

    def test_model_subset(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, m(c1=1, c2=2))
        assert values == dict(c1=1, c2=2)

    def test_model_excludes_pk(self):
        m = define_model(table1, model_type=T1)
        values = model_values(m, m(c1=1, c2=2, c3=3), excludes_pk=True)
        assert values == dict(c2=2, c3=3)

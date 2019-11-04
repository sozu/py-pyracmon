import pytest
from pyracmon.connection import Connection
from pyracmon.model import *
from pyracmon.mixin import CRUDMixin, read_row
from tests.db_api import PseudoAPI, PseudoConnection


table1 = Table("t1", [
    Column("c1", True, "seq"),
    Column("c2", False, None),
    Column("c3", False, None),
])

table2 = Table("t2", [
    Column("c1", True, "seq"),
    Column("c2", True, None),
    Column("c3", False, None),
])

class TestSelect:
    def test_all_columns(self):
        m = define_model(table1, [CRUDMixin])
        s = m.select()
        assert str(s) == "c1, c2, c3"
        v = s.consume([1, 2, 3])
        assert isinstance(v, m)
        assert (v.c1, v.c2, v.c3) == (1, 2, 3)

    def test_alias(self):
        m = define_model(table1, [CRUDMixin])
        s = m.select("t")
        assert str(s) == "t.c1, t.c2, t.c3"
        v = s.consume([1, 2, 3])
        assert isinstance(v, m)
        assert (v.c1, v.c2, v.c3) == (1, 2, 3)

    def test_includes(self):
        m = define_model(table1, [CRUDMixin])
        s = m.select("t", ["c1", "c3"])
        assert str(s) == "t.c1, t.c3"
        v = s.consume([1, 3])
        assert isinstance(v, m)
        assert (v.c1, v.c3) == (1, 3)

    def test_excludes(self):
        m = define_model(table1, [CRUDMixin])
        s = m.select("t", excludes = ["c2"])
        assert str(s) == "t.c1, t.c3"
        v = s.consume([1, 3])
        assert isinstance(v, m)
        assert (v.c1, v.c3) == (1, 3)


class TestReadRow:
    def test_read_row(self):
        m1 = define_model(table1, [CRUDMixin])
        m2 = define_model(table2, [CRUDMixin])
        s1 = m1.select()
        s2 = m2.select(excludes = ["c2"])
        r1, v1, r2, v2 = read_row([1, 2, 3, 4, 5, 6, 7], s1, (), s2, ())
        assert isinstance(r1, m1)
        assert (r1.c1, r1.c2, r1.c3) == (1, 2, 3)
        assert v1 == 4
        assert isinstance(r2, m2)
        assert (r2.c1, r2.c3) == (5, 6)
        assert v2 == 7


class TestQueryString:
    def _db(self):
        return PseudoAPI().connect()

    def test_insert(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.insert(db, dict(c1 = 1, c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "INSERT INTO t1 (c1, c2, c3) VALUES (?, ? * 2, ?)"
        assert list(db.params_list[0]) == [1, 2, 3]

    def test_insert_with_model(self):
        db = self._db()
        class LastSequences:
            @classmethod
            def last_sequences(cls, db, num):
                return [(table1.columns[0], 100)]
        m = define_model(table1, [LastSequences, CRUDMixin])

        model = m(c2 = 2, c3 = 3)
        m.insert(db, model, dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
        assert list(db.params_list[0]) == [2, 3]
        assert model.c1 == 100

    def test_update_by_pk(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_by_pks(self):
        db = self._db()
        m = define_model(table2, [CRUDMixin])

        m.update(db, dict(c1 = 1, c2 = 2), dict(c3 = 3))

        assert db.query_list[0] == "UPDATE t2 SET c3 = ? WHERE c1 = ? AND c2 = ?"
        assert list(db.params_list[0]) == [3, 1, 2]

    def test_update_with_model(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.update(db, 1, m(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_delete_by_pk(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.delete(db, 1)

        assert db.query_list[0] == "DELETE FROM t1 WHERE c1 = ?"
        assert list(db.params_list[0]) == [1]

    def test_delete_by_pks(self):
        db = self._db()
        m = define_model(table2, [CRUDMixin])

        m.delete(db, dict(c1 = 1, c2 = 2))

        assert db.query_list[0] == "DELETE FROM t2 WHERE c1 = ? AND c2 = ?"
        assert list(db.params_list[0]) == [1, 2]
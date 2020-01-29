import pytest
from pyracmon.connection import Connection
from pyracmon.model import *
from pyracmon.mixin import CRUDMixin, read_row, RowValues
from pyracmon.query import Q
from tests.db_api import PseudoAPI, PseudoConnection


table1 = Table("t1", [
    Column("c1", int, None, True, False, "seq"),
    Column("c2", int, None, False, False, None),
    Column("c3", int, None, False, False, None),
])

table2 = Table("t2", [
    Column("c1", int, None, True, False, "seq"),
    Column("c2", int, None, True, False, None),
    Column("c3", int, None, False, False, None),
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


class TestCount:
    def test_count_all(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[3]])
        r = m.count(db)

        assert db.query_list[0] == "SELECT COUNT(*) FROM t1 "
        assert list(db.params_list[0]) == []
        assert r == 3

    def test_count_where(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[3]])
        r = m.count(db, lambda m: Q.of(f"c3 > {m()}", 5))

        assert db.query_list[0] == "SELECT COUNT(*) FROM t1 WHERE c3 > ?"
        assert list(db.params_list[0]) == [5]
        assert r == 3


class TestFetch:
    def test_fetch(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 3]])
        r = m.fetch(db, 1)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c1 = ?"
        assert list(db.params_list[0]) == [1]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)

    def test_multiple_pks(self):
        db = PseudoAPI().connect()
        m = define_model(table2, [CRUDMixin])

        db.reserve([[1, "abc", 3]])
        r = m.fetch(db, dict(c1 = 1, c2 = "abc"))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t2 WHERE c1 = ? AND c2 = ?"
        assert list(db.params_list[0]) == [1, "abc"]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)

    def test_empty(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([])
        r = m.fetch(db, 1)

        assert r is None

    def test_lock(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 3]])
        r = m.fetch(db, 1, lock = "FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c1 = ? FOR UPDATE"
        assert list(db.params_list[0]) == [1]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)


class TestFetchWhere:
    def test_no_condition(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1"
        assert list(db.params_list[0]) == []
        assert (rs[0].c1, rs[0].c2, rs[0].c3) == (1, "abc", 10)
        assert (rs[1].c1, rs[1].c2, rs[1].c3) == (2, "def", 20)

    def test_with_condition(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, lambda m: Q.of(f"c3 > {m()}", 5))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c3 > ?"
        assert list(db.params_list[0]) == [5]

    def test_with_condition_simple(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, Q.of(f"c3 > ?", 5))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c3 > ?"
        assert list(db.params_list[0]) == [5]

    def test_asc_order(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, orders = dict(c1 = True))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 ASC"

    def test_desc_order(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, orders = dict(c1 = False))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 DESC"

    def test_multiple_orders(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, orders = dict(c1 = True, c3 = False))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 ASC, c3 DESC"

    def test_limit(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, limit = 10)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 LIMIT ?"
        assert list(db.params_list[0]) == [10]

    def test_offset(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, offset = 20)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 OFFSET ?"
        assert list(db.params_list[0]) == [20]

    def test_lock(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, lambda m: Q.of(f"c3 > {m()}", 5), lock="FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c3 > ? FOR UPDATE"
        assert list(db.params_list[0]) == [5]

    def test_all_args(self):
        db = PseudoAPI().connect()
        m = define_model(table1, [CRUDMixin])

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = m.fetch_where(db, lambda m: Q.of(f"c3 > {m()}", 5), dict(c1 = True, c3 = False), limit = 10, offset = 20, lock = "FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c3 > ? ORDER BY c1 ASC, c3 DESC LIMIT ? OFFSET ? FOR UPDATE"
        assert list(db.params_list[0]) == [5, 10, 20]


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

        assert db.query_list[0] == "UPDATE t2 SET c3 = ? WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [3, 1, 2]

    def test_update_with_model(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.update(db, 1, m(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_where(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        def condition(mk):
            q = Q(c2 = "abc", c3 = 10)
            return q.c2(f"c2 = {mk()}") | q.c3(f"c3 > {mk()}")
        m.update_where(db, m(c2 = 2, c3 = 3), condition)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_where_simple(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.update_where(db, m(c2 = 2, c3 = 3), Q.of("c2 = ?", "abc") | Q.of("c3 > ?", 10))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_all_ng(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        with pytest.raises(ValueError):
            m.update_where(db, m(c2 = 2, c3 = 3), lambda x: Q.of("", []))

    def test_update_all_ok(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.update_where(db, m(c2 = 2, c3 = 3), lambda x: Q.of("", []), allow_all = True)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? "
        assert list(db.params_list[0]) == [2, 3]

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

        assert db.query_list[0] == "DELETE FROM t2 WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [1, 2]

    def test_delete_where(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        mk = db.helper.marker()
        q = Q(c2 = "abc", c3 = 10)
        m.delete_where(db, lambda mk: q.c2(f"c2 = {mk()}") | q.c3(f"c3 > {mk()}"))

        assert db.query_list[0] == "DELETE FROM t1 WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == ["abc", 10]

    def test_delete_where_simple(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        mk = db.helper.marker()
        q = Q(c2 = "abc", c3 = 10)
        m.delete_where(db, Q.of(f"c2 = ?", "abc") | Q.of(f"c3 > ?", 10))

        assert db.query_list[0] == "DELETE FROM t1 WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == ["abc", 10]

    def test_delete_all_ng(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        with pytest.raises(ValueError):
            m.delete_where(db, lambda mk: Q.of("", []))

    def test_delete_all_ok(self):
        db = self._db()
        m = define_model(table1, [CRUDMixin])

        m.delete_where(db, lambda mk: Q.of("", []), allow_all = True)

        assert db.query_list[0] == "DELETE FROM t1 "
        assert list(db.params_list[0]) == []


class TestRowValues:
    def test_attributes(self):
        m1 = define_model(table1, [CRUDMixin])
        m2 = define_model(table2, [CRUDMixin])

        a1 = m1.select("a1", ["c1", "c2"])
        a2 = m2.select("a2", ["c1", "c2", "c3"])

        rv = RowValues([a1, a2, (), "a3", (), "a4"])
        rv.append(m1(c1 = 1))
        rv.append(m2(c1 = 2))
        for i in range(0, 4):
            rv.append(i+3)

        assert isinstance(rv.a1, m1)
        assert rv.a1.c1 == 1
        assert isinstance(rv.a2, m2)
        assert rv.a2.c1 == 2
        assert [rv[i] for i in range(2, 6)] == [3, 4, 5, 6]
        assert rv.a3 == 4
        assert rv.a4 == 6

    def test_reading_result(self):
        m1 = define_model(table1, [CRUDMixin])
        m2 = define_model(table2, [CRUDMixin])

        exps = m1.select("a1", ["c1", "c2"]), m2.select("a2", ["c1", "c2", "c3"])

        row = [1, "c2", 2, "c3", "c4", 3, 4, 5, 6]
        rv = read_row(row, *exps, (), "a3", (), "a4")

        assert isinstance(rv.a1, m1)
        assert (rv.a1.c1, rv.a1.c2) == (1, "c2")
        assert isinstance(rv.a2, m2)
        assert (rv.a2.c1, rv.a2.c2, rv.a2.c3) == (2, "c3", "c4")
        assert [rv[i] for i in range(2, 6)] == [3, 4, 5, 6]
        assert rv.a3 == 4
        assert rv.a4 == 6


class TestExpressions:
    def test_create_expand(self):
        m1 = define_model(table1, [CRUDMixin])

        a1 = m1.select("a1")
        a2 = m1.select("a2")
        a3 = m1.select()

        exp = a1 + a2 + a3

        assert exp.a1 == a1
        assert exp.a2 == a2
        assert exp.t1 == a3

        def f(*args):
            return list(args)
        assert f(*exp) == [a1, a2, a3]

    def test_reading_result(self):
        m1 = define_model(table1, [CRUDMixin])
        m2 = define_model(table2, [CRUDMixin])

        exp = m1.select("a1", ["c1", "c2"]) + m2.select("a2", ["c1", "c2", "c3"])

        row = [1, "c2", 2, "c3", "c4", 3, 4, 5, 6]
        rv = read_row(row, *exp, (), "a3", (), "a4")

        assert isinstance(rv.a1, m1)
        assert (rv.a1.c1, rv.a1.c2) == (1, "c2")
        assert isinstance(rv.a2, m2)
        assert (rv.a2.c1, rv.a2.c2, rv.a2.c3) == (2, "c3", "c4")
        assert [rv[i] for i in range(2, 6)] == [3, 4, 5, 6]
        assert rv.a3 == 4
        assert rv.a4 == 6
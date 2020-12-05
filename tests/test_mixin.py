import pytest
from pyracmon.connection import Connection
from pyracmon.model import Table, Column, define_model
from pyracmon.mixin import CRUDMixin, read_row, RowValues
from pyracmon.query import Q
from tests.db_api import PseudoAPI, PseudoConnection


class LastSequences:
    sequences = []

    @classmethod
    def last_sequences(cls, db, num):
        return cls.sequences.pop(0) if cls.sequences else []


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

model1 = define_model(table1, [LastSequences, CRUDMixin])
model2 = define_model(table2, [LastSequences, CRUDMixin])


class TestCount:
    def test_count_all(self):
        db = PseudoAPI().connect()

        db.reserve([[3]])
        r = model1.count(db)

        assert db.query_list[0] == "SELECT COUNT(*) FROM t1"
        assert list(db.params_list[0]) == []
        assert r == 3

    def test_count_where(self):
        db = PseudoAPI().connect()

        db.reserve([[3]])
        r = model1.count(db, Q.gt(c3 = 5) & Q.lt(c2 = 4))

        assert db.query_list[0] == "SELECT COUNT(*) FROM t1 WHERE (c3 > ?) AND (c2 < ?)"
        assert list(db.params_list[0]) == [5, 4]
        assert r == 3


class TestFetch:
    def test_fetch(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 3]])
        r = model1.fetch(db, 1)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c1 = ?"
        assert list(db.params_list[0]) == [1]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)

    def test_multiple_pks(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 3]])
        r = model2.fetch(db, dict(c1 = 1, c2 = "abc"))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t2 WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [1, "abc"]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)

    def test_empty(self):
        db = PseudoAPI().connect()

        db.reserve([])
        r = model1.fetch(db, 1)

        assert r is None

    def test_lock(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 3]])
        r = model1.fetch(db, 1, lock = "FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE c1 = ? FOR UPDATE"
        assert list(db.params_list[0]) == [1]
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)


class TestFetchWhere:
    def test_no_condition(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1"
        assert list(db.params_list[0]) == []
        assert (rs[0].c1, rs[0].c2, rs[0].c3) == (1, "abc", 10)
        assert (rs[1].c1, rs[1].c2, rs[1].c3) == (2, "def", 20)

    def test_with_condition(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, Q.gt(c3 = 5) & Q.lt(c2 = 3))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE (c3 > ?) AND (c2 < ?)"
        assert list(db.params_list[0]) == [5, 3]

    def test_asc_order(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, orders = dict(c1 = True))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 ASC"

    def test_desc_order(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, orders = dict(c1 = False))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 DESC"

    def test_multiple_orders(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, orders = dict(c1 = True, c3 = False))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 ORDER BY c1 ASC, c3 DESC"

    def test_limit(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, limit = 10)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 LIMIT ?"
        assert list(db.params_list[0]) == [10]

    def test_offset(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, offset = 20)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 OFFSET ?"
        assert list(db.params_list[0]) == [20]

    def test_lock(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, Q.gt(c3 = 5) & Q.lt(c2 = 3), lock="FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE (c3 > ?) AND (c2 < ?) FOR UPDATE"
        assert list(db.params_list[0]) == [5, 3]

    def test_all_args(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])
        rs = model1.fetch_where(db, Q.gt(c3 = 5) & Q.lt(c2 = 3), dict(c1 = True, c3 = False), limit = 10, offset = 20, lock = "FOR UPDATE")

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1 WHERE (c3 > ?) AND (c2 < ?) ORDER BY c1 ASC, c3 DESC LIMIT ? OFFSET ? FOR UPDATE"
        assert list(db.params_list[0]) == [5, 3, 10, 20]


class TestInsert:
    def test_insert(self):
        db = PseudoAPI().connect()

        r = model1.insert(db, dict(c1 = 1, c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "INSERT INTO t1 (c1, c2, c3) VALUES (?, ? * 2, ?)"
        assert list(db.params_list[0]) == [1, 2, 3]
        assert (r.c1, r.c2, r.c3) == (1, 2, 3)

    def test_set_pk(self):
        db = PseudoAPI().connect()

        try:
            LastSequences.sequences.append([(table1.columns[0], 100)])
            m = model1(c2 = 2, c3 = 3)
            r = model1.insert(db, m, dict(c2 = lambda h: f"{h} * 2"))
        finally:
            LastSequences.sequences.clear()

        assert r is m
        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
        assert list(db.params_list[0]) == [2, 3]
        assert (m.c1, m.c2, m.c3) == (100, 2, 3)


class TestUpdate:
    def test_update_by_pk(self):
        db = PseudoAPI().connect()

        model1.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_by_pks(self):
        db = PseudoAPI().connect()

        model2.update(db, dict(c1 = 1, c2 = 2), dict(c3 = 3))

        assert db.query_list[0] == "UPDATE t2 SET c3 = ? WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [3, 1, 2]

    def test_update_with_model(self):
        db = PseudoAPI().connect()

        model1.update(db, 1, model1(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_exclude_pk(self):
        db = PseudoAPI().connect()

        model1.update(db, 1, model1(c1 = 5, c2 = 2, c3 = 3))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_by_expression(self):
        db = PseudoAPI().connect()

        model1.update(db, 1, dict(c2 = Q.of("c2 + $_", 10), c3 = Q.of("c2 * c3")), dict(c2 = lambda h: f"({h}) * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = (c2 + ?) * 2, c3 = c2 * c3 WHERE c1 = ?"
        assert list(db.params_list[0]) == [10, 1]


class TestUpdateWhere:
    def test_update_where(self):
        db = PseudoAPI().connect()

        model1.update_where(db, model1(c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_exclude_pk(self):
        db = PseudoAPI().connect()

        model1.update_where(db, model1(c1 = 5, c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_by_dict(self):
        db = PseudoAPI().connect()

        model1.update_where(db, dict(c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_by_expression(self):
        db = PseudoAPI().connect()

        model1.update_where(db, dict(c2 = Q.of("c2 + $_", 5), c3 = Q.of("c2 * c3")), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert db.query_list[0] == "UPDATE t1 SET c2 = c2 + ?, c3 = c2 * c3 WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [5, "abc", 10]

    def test_update_qualified(self):
        db = PseudoAPI().connect()

        model1.update_where(db, model1(c2 = Q.of("c2 + $_", 5), c3 = 3), Q.of(), dict(c2 = lambda h: f"({h}) * 2", c3 = lambda h: f"abs({h})"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = (c2 + ?) * 2, c3 = abs(?)"
        assert list(db.params_list[0]) == [5, 3]

    def test_update_all_ng(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model1.update_where(db, model1(c2 = 2, c3 = 3), Q.of(), allow_all = False)

    def test_update_all_ok(self):
        db = PseudoAPI().connect()

        model1.update_where(db, model1(c2 = 2, c3 = 3), Q.of())

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ?"
        assert list(db.params_list[0]) == [2, 3]


class TestDelete:
    def test_delete_by_pk(self):
        db = PseudoAPI().connect()

        model1.delete(db, 1)

        assert db.query_list[0] == "DELETE FROM t1 WHERE c1 = ?"
        assert list(db.params_list[0]) == [1]

    def test_delete_by_pks(self):
        db = PseudoAPI().connect()

        model2.delete(db, dict(c1 = 1, c2 = 2))

        assert db.query_list[0] == "DELETE FROM t2 WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [1, 2]


class TestDeleteWhere:
    def test_delete_where(self):
        db = PseudoAPI().connect()

        model1.delete_where(db, Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert db.query_list[0] == "DELETE FROM t1 WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == ["abc", 10]

    def test_delete_all_ng(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model1.delete_where(db, Q.of(), allow_all = False)

    def test_delete_all_ok(self):
        db = PseudoAPI().connect()

        model1.delete_where(db, Q.of())

        assert db.query_list[0] == "DELETE FROM t1"
        assert list(db.params_list[0]) == []
import pytest
from pyracmon.connection import Connection
from pyracmon.model import Table, Column, define_model, COLUMN, Model
from pyracmon.query import Q
from tests.db_api import PseudoAPI, PseudoConnection
from pyracmon.mixin import *


class CRUDInternal:
    sequences = []

    @classmethod
    def last_sequences(cls, db, num):
        return cls.sequences.pop(0) if cls.sequences else []

    @classmethod
    def support_returning(cls, db: Connection) -> bool:
        return True


table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, False, None, None, False),
    Column("c3", int, None, False, None, None, True),
])
class T1(Model, CRUDMixin): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN

table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, None, None, True),
    Column("c3", int, None, False, None, None, False),
])
class T2(Model, CRUDMixin): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN

model1 = define_model(table1, [CRUDInternal, CRUDMixin], model_type=T1)
model2 = define_model(table2, [CRUDInternal, CRUDMixin], model_type=T2)


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
        assert r
        assert (r.c1, r.c2, r.c3) == (1, "abc", 3)

    def test_multiple_pks(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 3]])
        r = model2.fetch(db, dict(c1 = 1, c2 = "abc"))

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t2 WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [1, "abc"]
        assert r
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
        assert r
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


class TestFetchOne:
    def test_singular(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10]])
        r = model1.fetch_one(db)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1"
        assert list(db.params_list[0]) == []
        assert r
        assert (r.c1, r.c2, r.c3) == (1, "abc", 10)

    def test_empty(self):
        db = PseudoAPI().connect()

        db.reserve([])
        r = model1.fetch_one(db)

        assert db.query_list[0] == "SELECT c1, c2, c3 FROM t1"
        assert list(db.params_list[0]) == []
        assert r is None

    def test_multiple(self):
        db = PseudoAPI().connect()

        db.reserve([[1, "abc", 10], [2, "def", 20]])

        with pytest.raises(ValueError):
            model1.fetch_one(db)


class TestInsert:
    def test_insert(self):
        db = PseudoAPI().connect()

        r = model1.insert(db, dict(c1 = 1, c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "INSERT INTO t1 (c1, c2, c3) VALUES (?, ? * 2, ?)"
        assert list(db.params_list[0]) == [1, 2, 3]
        assert (r.c1, r.c2, r.c3) == (1, 2, 3)

    def test_insert_returning(self):
        db = PseudoAPI().connect()

        db.reserve([[1, 2, 3]])
        r = model1.insert(db, dict(c1 = 1, c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"), returning=True)

        assert db.query_list[0] == "INSERT INTO t1 (c1, c2, c3) VALUES (?, ? * 2, ?) RETURNING *"
        assert list(db.params_list[0]) == [1, 2, 3]
        assert (r.c1, r.c2, r.c3) == (1, 2, 3)

    def test_set_pk(self):
        db = PseudoAPI().connect()

        try:
            CRUDInternal.sequences.append([(table1.columns[0], 100)])
            m = model1(c2 = 2, c3 = 3)
            r = model1.insert(db, m, dict(c2 = lambda h: f"{h} * 2"))
        finally:
            CRUDInternal.sequences.clear()

        assert r is m
        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
        assert list(db.params_list[0]) == [2, 3]
        assert (m.c1, m.c2, m.c3) == (100, 2, 3)

    def test_set_pk_returning(self):
        db = PseudoAPI().connect()
        db.reserve([[100, 2, 3]])

        m = model1(c2 = 2, c3 = 3)
        r = model1.insert(db, m, dict(c2 = lambda h: f"{h} * 2"), returning=True)

        assert r is not m
        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?) RETURNING *"
        assert list(db.params_list[0]) == [2, 3]
        assert (r.c1, r.c2, r.c3) == (100, 2, 3)

    def test_insert_by_expression(self):
        db = PseudoAPI().connect()

        r = model1.insert(db, dict(c1=1, c2=Expression("now()", []), c3=Expression("$_ + $_", [3, 4])))

        assert db.query_list[0] == "INSERT INTO t1 (c1, c2, c3) VALUES (?, now(), ? + ?)"
        assert list(db.params_list[0]) == [1, 3, 4]


class TestInsertMany:
    def test_insert(self):
        db = PseudoAPI().connect()

        try:
            CRUDInternal.sequences.append([(table1.columns[0], 100)])
            rs = model1.insert_many(db, [dict(c2=2, c3=3), dict(c2=5, c3=6)], dict(c2 = lambda h: f"{h} * 2"))
        finally:
            CRUDInternal.sequences.clear()

        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
        assert list(db.params_list) == [[2, 3], [5, 6]]
        assert (rs[0].c1, rs[0].c2, rs[0].c3) == (99, 2, 3)
        assert (rs[1].c1, rs[1].c2, rs[1].c3) == (100, 5, 6)

    def test_insert_returning(self):
        db = PseudoAPI().connect()
        db.reserve([[99, 4, 3], [100, 10, 6]])

        try:
            CRUDInternal.sequences.append([(table1.columns[0], 100)])
            rs = model1.insert_many(db, [dict(c2=2, c3=3), dict(c2=5, c3=6)], dict(c2 = lambda h: f"{h} * 2"), returning=True)
        finally:
            CRUDInternal.sequences.clear()

        assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
        assert list(db.params_list) == [[2, 3], [5, 6], [99, 100]] # Selecting after insert.
        assert (rs[0].c1, rs[0].c2, rs[0].c3) == (99, 4, 3)
        assert (rs[1].c1, rs[1].c2, rs[1].c3) == (100, 10, 6)

    # Not use RETURNING clause
    #def test_insert_returning(self):
    #    db = PseudoAPI().connect()

    #    db.reserve([[1, 2, 3], [4, 5, 6]])
    #    rs = model1.insert_many(db, [dict(c2=2, c3=3), dict(c2=5, c3=6)], dict(c2 = lambda h: f"{h} * 2"), returning=True)

    #    assert db.query_list[0] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?) RETURNING *"
    #    assert list(db.params_list) == [[2, 3], [5, 6]]

    def test_insert_inconsistent_columns(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model1.insert_many(db, [dict(c1=1, c3=3), dict(c1=4, c2=5)])

    def test_insert_by_expression(self):
        db = PseudoAPI().connect()

        model2.insert_many(db, [
            dict(c1=1, c2=Expression("now()", []), c3=Expression("$_ + $_", [3, 4])),
            dict(c1=2, c2=Expression("dummy()", []), c3=Expression("", [5, 6])),
        ])

        assert db.query_list[0] == "INSERT INTO t2 (c1, c2, c3) VALUES (?, now(), ? + ?)"
        assert db.params_list == [[1, 3, 4], [2, 5, 6]]


class TestUpdate:
    def test_update_by_pk(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model1.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert r is True
        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_by_pks(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model2.update(db, dict(c1 = 1, c2 = 2), dict(c3 = 3))

        assert r is True
        assert db.query_list[0] == "UPDATE t2 SET c3 = ? WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list[0]) == [3, 1, 2]

    def test_update_with_model(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model1.update(db, 1, model1(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert r is True
        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_exclude_pk(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model1.update(db, 1, model1(c1 = 5, c2 = 2, c3 = 3))

        assert r is True
        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE c1 = ?"
        assert list(db.params_list[0]) == [2, 3, 1]

    def test_update_by_expression(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model1.update(db, 1, dict(c2 = Q.of("c2 + $_", 10), c3 = Q.of("c2 * c3")), dict(c2 = lambda h: f"({h}) * 2"))

        assert r is True
        assert db.query_list[0] == "UPDATE t1 SET c2 = (c2 + ?) * 2, c3 = c2 * c3 WHERE c1 = ?"
        assert list(db.params_list[0]) == [10, 1]

    def test_update_not_found(self):
        db = PseudoAPI().connect()

        db.rowcount = 0
        r = model1.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"))

        assert r is False

    def test_update_returning(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        db.reserve([[1, 2, 3]])
        r = model1.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"), returning=True)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ? RETURNING *"
        assert list(db.params_list[0]) == [2, 3, 1]
        assert r is not None
        assert (r.c1, r.c2, r.c3) == (1, 2, 3)

    def test_update_returning_empty(self):
        db = PseudoAPI().connect()

        db.rowcount = 1
        r = model1.update(db, 1, dict(c2 = 2, c3 = 3), dict(c2 = lambda h: f"{h} * 2"), returning=True)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ? RETURNING *"
        assert list(db.params_list[0]) == [2, 3, 1]
        assert r is None


class TestUpdateWhere:
    def test_update_where(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, model1(c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_exclude_pk(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, model1(c1 = 5, c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_by_dict(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, dict(c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]

    def test_update_by_expression(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, dict(c2 = Q.of("c2 + $_", 5), c3 = Q.of("c2 * c3")), Q.eq(c2 = "abc") | Q.gt(c3 = 10))

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = c2 + ?, c3 = c2 * c3 WHERE (c2 = ?) OR (c3 > ?)"
        assert list(db.params_list[0]) == [5, "abc", 10]

    def test_update_qualified(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, dict(c2 = Q.of("c2 + $_", 5), c3 = 3), Q.of(), dict(c2 = lambda h: f"({h}) * 2", c3 = lambda h: f"abs({h})"))

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = (c2 + ?) * 2, c3 = abs(?)"
        assert list(db.params_list[0]) == [5, 3]

    def test_update_all_ng(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model1.update_where(db, model1(c2 = 2, c3 = 3), Q.of(), allow_all = False)

    def test_update_all_ok(self):
        db = PseudoAPI().connect()

        db.rowcount = 3
        r = model1.update_where(db, model1(c2 = 2, c3 = 3), Q.of())

        assert r == 3
        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ?"
        assert list(db.params_list[0]) == [2, 3]

    def test_update_returning(self):
        db = PseudoAPI().connect()

        db.reserve([[1, 2, 3], [4, 5, 6]])
        r = model1.update_where(db, model1(c2 = 2, c3 = 3), Q.eq(c2 = "abc") | Q.gt(c3 = 10), returning=True)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ?, c3 = ? WHERE (c2 = ?) OR (c3 > ?) RETURNING *"
        assert list(db.params_list[0]) == [2, 3, "abc", 10]
        assert len(r) == 2
        assert (r[0].c1, r[0].c2, r[0].c3) == (1, 2, 3)
        assert (r[1].c1, r[1].c2, r[1].c3) == (4, 5, 6)


class TestUpdateMany:
    def test_update(self):
        db = PseudoAPI().connect()

        rs = model1.update_many(db, [dict(c1=1, c2=2, c3=3), dict(c1=4, c2=5, c3=6)], dict(c2 = lambda h: f"{h} * 2"))

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list) == [[2, 3, 1], [5, 6, 4]]

    def test_update_returning(self):
        db = PseudoAPI().connect()

        db.reserve([[1, 2, 3], [4, 5, 6]])
        rs = model1.update_many(db, [dict(c1=1, c2=2, c3=3), dict(c1=4, c2=5, c3=6)], dict(c2 = lambda h: f"{h} * 2"), returning=True)

        assert db.query_list[0] == "UPDATE t1 SET c2 = ? * 2, c3 = ? WHERE c1 = ?"
        assert list(db.params_list) == [[2, 3, 1], [5, 6, 4], [1, 4]] # Selecting after update
        #assert (rs[0].c1, rs[0].c2, rs[0].c3) == (1, 2, 3)
        #assert (rs[1].c1, rs[1].c2, rs[1].c3) == (4, 5, 6)

    def test_update_multi_pk(self):
        db = PseudoAPI().connect()

        rs = model2.update_many(db, [dict(c1=1, c2=2, c3=3), dict(c1=4, c2=5, c3=6)])

        assert db.query_list[0] == "UPDATE t2 SET c3 = ? WHERE (c1 = ?) AND (c2 = ?)"
        assert list(db.params_list) == [[3, 1, 2], [6, 4, 5]]

    def test_update_pk_missing(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model2.update_many(db, [dict(c1=1, c3=3), dict(c1=4, c3=6)])

    def test_update_inconsistent_columns(self):
        db = PseudoAPI().connect()

        with pytest.raises(ValueError):
            model1.update_many(db, [dict(c1=1, c3=3), dict(c1=4, c2=5)])


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


class TestDeleteMany:
    def test_delete_by_pk(self):
        db = PseudoAPI().connect()

        model1.delete_many(db, [1, 2, 3])

        assert db.query_list[0] == "DELETE FROM t1 WHERE c1 = ?"
        assert db.params_list == [[1], [2], [3]]

    def test_delete_by_pks(self):
        db = PseudoAPI().connect()

        model2.delete_many(db, [dict(c1=1, c2=2), dict(c1=3, c2=4)])

        assert db.query_list[0] == "DELETE FROM t2 WHERE (c1 = ?) AND (c2 = ?)"
        assert db.params_list == [[1, 2], [3, 4]]

    def test_delete_by_records(self):
        db = PseudoAPI().connect()

        model1.delete_many(db, [dict(c1=1, c2=2), dict(c1=2, c3=4)])

        assert db.query_list[0] == "DELETE FROM t1 WHERE c1 = ?"
        assert db.params_list == [[1], [2]]

    def test_delete_by_models(self):
        db = PseudoAPI().connect()

        model1.delete_many(db, [model1(c1=1, c2=2), model1(c1=2, c3=4)])

        assert db.query_list[0] == "DELETE FROM t1 WHERE c1 = ?"
        assert db.params_list == [[1], [2]]
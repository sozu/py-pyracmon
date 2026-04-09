import sys
from typing import NamedTuple, TYPE_CHECKING
import psycopg2
import pymysql
import pytest
from itertools import combinations_with_replacement
from pyracmon import declare_models
from pyracmon.connection import Connection, connect
from pyracmon.model import Model, COLUMN
from pyracmon.testing import truncate
from pyracmon.dialect.shared import TruncateMixin
from pyracmon.dialect import postgresql, mysql
from pyracmon.mixin import *


if TYPE_CHECKING:
    class m(NamedTuple):
        class t1(Model, TruncateMixin, CRUDMixin): c11: int = COLUMN; c12: int = COLUMN; c13: str = COLUMN
        class t2(Model, TruncateMixin, CRUDMixin): c21: int = COLUMN; c22: int = COLUMN; c23: str = COLUMN
        class t3(Model, TruncateMixin, CRUDMixin): c31: int = COLUMN; c32: int = COLUMN; c33: str = COLUMN
        class t4(Model, TruncateMixin, CRUDMixin): c41: int = COLUMN; c42: int = COLUMN; c43: int = COLUMN
else:
    from tests import models as m


def _connect_postgresql():
    return connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )


def _connect_mysql():
    return connect(
        pymysql,
        db = "pyracmon_test",
        user = "root",
        password = "root",
        host = "mysql",
        port = 3306,
    )


last_dialect: Optional[str] = None


@pytest.fixture(params=["postgresql", "mysql"])
#@pytest.fixture(params=["postgresql"])
#@pytest.fixture(params=["mysql"])
def db(request):
    global last_dialect
    if request.param == "postgresql":
        db = _connect_postgresql()
        dialect = postgresql
    elif request.param == "mysql":
        db = _connect_mysql()
        dialect = mysql
    else:
        raise ValueError(f"Unexpected DBMS: {request.param}")

    if 't1' not in dir(m) or last_dialect != request.param:
        declare_models(dialect, db, 'tests.models')

    last_dialect = request.param

    truncate(db, m.t4, m.t3, m.t2, m.t1)
    try:
        db.stmt().execute("begin")
        yield db
    finally:
        db.rollback()
        db.close()


class TestCount:
    def fixture(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

    def test_count_all(self, db: Connection):
        self.fixture(db)

        r = m.t1.count(db)

        assert r == 3

    def test_count_where(self, db: Connection):
        self.fixture(db)

        r = m.t1.count(db, Q.gt(c12 = 2) & Q.lt(c12 = 4))

        assert r == 1


class TestFetch:
    def test_fetch(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = m.t1.fetch(db, 2)

        assert r is not None
        assert (r.c11, r.c12, r.c13) == (2, 3, 'def')

    def test_not_found(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = m.t1.fetch(db, 0)

        assert r is None

    def test_multiple_pks(self, db: Connection):
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        r = m.t2.fetch(db, dict(c21=2, c22=3))

        assert r is not None
        assert (r.c21, r.c22, r.c23) == (2, 3, 'def')

    def test_multiple_not_found(self, db: Connection):
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        r = m.t2.fetch(db, dict(c21=1, c22=3))

        assert r is None

    def test_mssing_pk(self, db: Connection):
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

        with pytest.raises(ValueError):
            m.t2.fetch(db, dict(c21=1))


class TestFetchMany:
    def test_fetch(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = m.t1.fetch_many(db, [1, 3, 5])

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi')]

    def test_duplicate(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi')")

        r = m.t1.fetch_many(db, [1, 3, 5, 1])

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi'), m.t1(c11=1, c12=2, c13='abc')]

    def test_pks(self, db: Connection):
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi'), (4, 5, 'jkl'), (5, 6, 'mno')")

        r = m.t2.fetch_many(db, [dict(c21=3, c22=4), dict(c21=0, c22=0), dict(c21=5, c22=6), dict(c21=1, c22=2)])

        assert [v.c21 for v in r] == [3, 5, 1]

    def test_pages(self, db: Connection):
        values = ', '.join(f"({v12}, '{v13}')" for v12, v13 in [(i+1, f"c{i+1}") for i in range(50)])
        db.stmt().execute(f"insert into t1 (c12, c13) values {values}")

        r = m.t1.fetch_many(db, range(1, 30, 2), per_page=5)

        assert [v.c11 for v in r] == [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]


class TestFetchWhere:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    def test_no_condition(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_where(db)

        assert len(r) == 5
        assert {1, 2, 3, 4, 5} == {v.c11 for v in r}

    def test_with_condition(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_where(db, Q.eq(c12=3))

        assert len(r) == 2
        assert {2, 4} == {v.c11 for v in r}

    def test_orders(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_where(db, orders=dict(c12=True, c11=False))

        assert len(r) == 5
        assert [5, 1, 4, 2, 3] == [v.c11 for v in r]

    def test_limit(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_where(db, orders=dict(c11=True), limit=3, offset=1)

        assert len(r) == 3
        assert [2, 3, 4] == [v.c11 for v in r]

    def test_various(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_where(db, Q.gt(c11=1) & Q.lt(c12=4), orders=dict(c11=True), limit=2, offset=1, lock="FOR UPDATE")

        assert len(r) == 2
        assert [4, 5] == [v.c11 for v in r]


class TestFetchOne:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    def test_singular(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_one(db, Q.eq(c13='jkl'))

        assert r is not None
        assert (r.c11, r.c12, r.c13) == (4, 3, 'jkl')

    def test_empty(self, db: Connection):
        self.prepare(db)

        r = m.t1.fetch_one(db, Q.eq(c13='pqr'))

        assert r is None

    def test_multiple(self, db: Connection):
        self.prepare(db)

        with pytest.raises(ValueError):
            m.t1.fetch_one(db, Q.eq(c12=3))


class TestInsert:
    def test_insert(self, db: Connection):
        r = m.t2.insert(db, dict(c21=1, c22=2, c23='abc'))

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert m.t2.count(db) == 1
        assert m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    def test_insert_model(self, db: Connection):
        r = m.t2.insert(db, m.t2(c21=1, c22=2, c23='abc'))

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert m.t2.count(db) == 1
        assert m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    def test_insert_returning(self, db: Connection):
        r = m.t2.insert(db, dict(c21=1, c22=2, c23='abc'), returning=True)

        assert (r.c21, r.c22, r.c23) == (1, 2, 'abc')

        assert m.t2.count(db) == 1
        assert m.t2.fetch(db, dict(c21=1, c22=2)) == m.t2(c21=1, c22=2, c23='abc')

    def test_set_pk(self, db: Connection):
        r = m.t1.insert(db, dict(c12=2, c13='abc'))

        assert (r.c11, r.c12, r.c13) == (1, 2, 'abc')

        assert m.t1.count(db) == 1
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    def test_set_pk_returning(self, db: Connection):
        r = m.t1.insert(db, dict(c12=2, c13='abc'), returning=True)

        assert (r.c11, r.c12, r.c13) == (1, 2, 'abc')

        assert m.t1.count(db) == 1
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    def test_insert_by_expression(self, db: Connection):
        r = m.t2.insert(db, dict(c21=1, c22=Q.of("$_ + $_", 11, 22), c23=Q.of("concat($_, 'xyz')", 'abc')), returning=True)

        if last_dialect == "postgresql":
            assert (r.c21, r.c22, r.c23) == (1, 33, 'abcxyz')

        assert m.t2.count(db) == 1
        assert m.t2.fetch(db, dict(c21=1, c22=33)) == m.t2(c21=1, c22=33, c23='abcxyz')


class TestInsertMany:
    def test_insert(self, db: Connection):
        r = m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3, c13='def'), dict(c12=4, c13='ghi')])

        assert len(r) == 3
        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

        assert m.t1.count(db) == 3
        assert m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

    def test_insert_returning(self, db: Connection):
        m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3, c13='def'), dict(c12=4, c13='ghi')], returning=True)

        assert m.t1.count(db) == 3
        assert m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]

    def test_insert_inconsistent_columns(self, db: Connection):
        with pytest.raises(ValueError):
            m.t1.insert_many(db, [dict(c12=2, c13='abc'), dict(c12=3), dict(c12=4, c13='ghi')], returning=True)

    def test_insert_by_expression(self, db: Connection):
        r = m.t1.insert_many(db, [
            dict(c12=Q.of("$_ + $_", 11, 22), c13=Q.of("concat($_, 'xyz')", 'abc')),
            dict(c12=Q.of("dummy()", 33, 44), c13=Q.of("", 'def')),
        ])

        assert m.t1.count(db) == 2
        assert m.t1.fetch_where(db, orders=dict(c11=True)) == [m.t1(c11=1, c12=33, c13='abcxyz'), m.t1(c11=2, c12=77, c13='defxyz')]


class TestUpdate:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    def test_update_by_pk(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 1, dict(c12=10, c13='xyz'))

        assert r is True
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    def test_update_by_pks(self, db: Connection):
        self.prepare(db)

        r = m.t2.update(db, dict(c21=2, c22=3), dict(c23='xyz'))

        assert r is True
        assert m.t2.fetch(db, dict(c21=2, c22=3)) == m.t2(c21=2, c22=3, c23='xyz')

    def test_update_with_model(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 1, m.t1(c12=10, c13='xyz'))

        assert r is True
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    def test_update_exclude_pk(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 1, m.t1(c11=100, c12=10, c13='xyz'))

        assert r is True
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    def test_update_by_expression(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 1, dict(c12=Q.of("c12 + $_", 10), c13=Q.of("concat($_, c13)", "xyz")), dict(c12=lambda h: f"({h}) * 2"))

        assert r is True
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=24, c13='xyzabc')

    def test_update_not_found(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 0, dict(c12=10, c13='xyz'))

        assert r is False
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')

    def test_update_returning(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 1, dict(c12=10, c13='xyz'), returning=True)

        assert r is not None
        assert r == m.t1(c11=1, c12=10, c13='xyz')
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=10, c13='xyz')

    def test_update_returning_empty(self, db: Connection):
        self.prepare(db)

        r = m.t1.update(db, 0, dict(c12=10, c13='xyz'), returning=True)

        assert r is None
        assert m.t1.fetch(db, 1) == m.t1(c11=1, c12=2, c13='abc')


class TestUpdateWhere:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    def test_update_where(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_exclude_pk(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_where(db, m.t1(c11=100, c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_by_dict(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_where(db, dict(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_by_expression(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_where(db, dict(c12=Q.of("c12 + $_", 10), c13=Q.of("concat($_, c13)", 'xyz')), Q.eq(c13='def') | Q.gt(c12=3),
                              dict(c12=lambda h: f"({h}) * 2"))

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=2, c13='abc'),
            m.t1(c11=2, c12=26, c13='xyzdef'),
            m.t1(c11=3, c12=28, c13='xyzghi'),
            m.t1(c11=4, c12=3, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_all_ng(self, db: Connection):
        self.prepare(db)

        with pytest.raises(ValueError):
            m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.of(), allow_all=False)

    def test_update_all_ok(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.of())

        assert r == 5
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=10, c13='xyz'),
            m.t1(c11=3, c12=10, c13='xyz'),
            m.t1(c11=4, c12=10, c13='xyz'),
            m.t1(c11=5, c12=10, c13='xyz'),
        ]

    def test_update_returning(self, db: Connection):
        self.prepare(db)


        if last_dialect == "postgresql":
            r = m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3), returning=True)

            assert len(r) == 2
            assert r == [
                m.t1(c11=2, c12=10, c13='xyz'),
                m.t1(c11=3, c12=10, c13='xyz'),
            ]

            act = m.t1.fetch_where(db, orders=dict(c11=True))
            assert act == [
                m.t1(c11=1, c12=2, c13='abc'),
                m.t1(c11=2, c12=10, c13='xyz'),
                m.t1(c11=3, c12=10, c13='xyz'),
                m.t1(c11=4, c12=3, c13='jkl'),
                m.t1(c11=5, c12=2, c13='mno'),
            ]
        else:
            with pytest.raises(NotImplementedError):
                r = m.t1.update_where(db, m.t1(c12=10, c13='xyz'), Q.eq(c13='def') | Q.gt(c12=3), returning=True)


class TestUpdateMany:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    def test_update(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_many(db, [dict(c11=1, c12=10, c13='xyz'), dict(c11=4, c12=11, c13='uvw'), dict(c11=0, c12=0, c13='---')])

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='uvw'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_partially(self, db: Connection):
        self.prepare(db)

        r = m.t1.update_many(db, [dict(c11=1, c12=10), dict(c11=4, c12=11), dict(c11=0, c12=0)])

        assert r == 2
        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='abc'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='jkl'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_returning(self, db: Connection):
        self.prepare(db)

        m.t1.update_many(db, [dict(c11=1, c12=10, c13='xyz'), dict(c11=4, c12=11, c13='uvw'), dict(c11=0, c12=0, c13='---')], returning=True)

        act = m.t1.fetch_where(db, orders=dict(c11=True))
        assert act == [
            m.t1(c11=1, c12=10, c13='xyz'),
            m.t1(c11=2, c12=3, c13='def'),
            m.t1(c11=3, c12=4, c13='ghi'),
            m.t1(c11=4, c12=11, c13='uvw'),
            m.t1(c11=5, c12=2, c13='mno'),
        ]

    def test_update_multi_pk(self, db: Connection):
        self.prepare(db)

        r = m.t2.update_many(db, [dict(c21=2, c22=3, c23='xyz'), dict(c21=0, c22=0, c23='---')])

        assert r == 1
        act = m.t2.fetch_where(db, orders=dict(c21=True))
        assert act == [
            m.t2(c21=1, c22=2, c23='abc'),
            m.t2(c21=2, c22=3, c23='xyz'),
            m.t2(c21=3, c22=4, c23='ghi'),
        ]

    def test_update_pk_missing(self, db: Connection):
        self.prepare(db)

        with pytest.raises(ValueError):
            m.t2.update_many(db, [dict(c21=2, c23='xyz')])

    def test_update_inconsistent_columns(self, db: Connection):
        self.prepare(db)

        with pytest.raises(ValueError):
            m.t1.update_many(db, [dict(c11=1, c12=10), dict(c11=4, c13='xyz')])


class TestDelete:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    def test_delete_by_pk(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete(db, 2)

        assert r is True
        assert m.t1.count(db) == 4
        assert m.t1.fetch(db, 2) is None

    def test_delete_by_pks(self, db: Connection):
        self.prepare(db)

        r = m.t2.delete(db, dict(c21=2, c22=3))

        assert r is True
        assert m.t2.count(db) == 2
        assert m.t2.fetch(db, dict(c21=2, c22=3)) is None

    def test_delete_returning(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete(db, 2, returning=True)

        assert r == m.t1(c11=2, c12=3, c13='def')
        assert m.t1.count(db) == 4
        assert m.t1.fetch(db, 2) is None

    def test_delete_not_found(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete(db, 0)

        assert r is False
        assert m.t1.count(db) == 5


class TestDeleteWhere:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")

    def test_delete_where(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_where(db, Q.eq(c13='def') | Q.gt(c12=3))

        assert r == 2
        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [1, 4, 5]

    def test_delete_returning(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_where(db, Q.eq(c13='def') | Q.gt(c12=3), returning=True)

        assert r == [m.t1(c11=2, c12=3, c13='def'), m.t1(c11=3, c12=4, c13='ghi')]
        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [1, 4, 5]

    def test_delete_all_ng(self, db: Connection):
        self.prepare(db)

        with pytest.raises(ValueError):
            m.t1.delete_where(db, Q.of(), allow_all=False)

    def test_delete_all_ok(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_where(db, Q.of())

        assert r == 5
        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == []


class TestDeleteMany:
    def prepare(self, db: Connection):
        db.stmt().execute("insert into t1 (c12, c13) values (2, 'abc'), (3, 'def'), (4, 'ghi'), (3, 'jkl'), (2, 'mno')")
        db.stmt().execute("insert into t2 (c21, c22, c23) values (1, 2, 'abc'), (2, 3, 'def'), (3, 4, 'ghi')")

    def test_delete_by_pk(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_many(db, [1, 3])

        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    def test_delete_by_pks(self, db: Connection):
        self.prepare(db)

        r = m.t2.delete_many(db, [dict(c21=1, c22=2), dict(c21=3, c22=4)])

        assert m.t2.fetch_where(db, orders=dict(c21=True)) == [m.t2(c21=2, c22=3, c23='def')]

    def test_delete_returning(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_many(db, [1, 3], returning=True)

        assert r == [m.t1(c11=1, c12=2, c13='abc'), m.t1(c11=3, c12=4, c13='ghi')]
        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    def test_delete_by_records(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_many(db, [dict(c11=1, c13='---'), dict(c11=3, c13='---')])

        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]

    def test_delete_by_models(self, db: Connection):
        self.prepare(db)

        r = m.t1.delete_many(db, [m.t1(c11=1, c13='---'), m.t1(c11=3, c13='---')])

        assert [v.c11 for v in m.t1.fetch_where(db, orders=dict(c11=True))] == [2, 4, 5]
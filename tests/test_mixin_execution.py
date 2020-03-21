import sys
import psycopg2
import pymysql
import pytest
from itertools import combinations_with_replacement
from tests import models as m
from pyracmon import *
from pyracmon.dialect import postgresql, mysql


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


class TestMixinExecution:
    def _execute(self, db):
        values = combinations_with_replacement("abc", 3)

        for i, v in enumerate(values):
            m.t1.insert(db, dict(c12 = i+1, c13 = ''.join(v)))

        assert m.t1.count(db) == 10
        assert m.t1.count(db, Q.lt(c12 = 4)) == 3

        r = m.t1.fetch(db, 3)
        assert (r.c11, r.c12, r.c13) == (3, 3, "aac")
        rs = m.t1.fetch_where(db, Q.lt(c12 = 4))
        assert [(r.c11, r.c12, r.c13) for r in rs] \
            == [(1, 1, "aaa"), (2, 2, "aab"), (3, 3, "aac")]
        rs = m.t1.fetch_where(db, Q.like(c13 = "ab"), orders = dict(c11 = True))
        assert [(r.c11, r.c12, r.c13) for r in rs] \
            == [(2, 2, "aab"), (4, 4, "abb"), (5, 5, "abc")]

        m.t1.update(db, 5, dict(c12 = 50))
        r = m.t1.fetch(db, 5)
        assert (r.c11, r.c12, r.c13) == (5, 50, "abc")

        m.t1.delete(db, 7)
        assert m.t1.count(db) == 9
        m.t1.delete_where(db, Q.in_(c12 = [2, 4, 6]))
        assert m.t1.count(db) == 6

        rs = m.t1.fetch_where(db, orders = dict(c11 = True))
        assert [(r.c11, r.c12, r.c13) for r in rs] \
            == [(1, 1, "aaa"), (3, 3, "aac"), (5, 50, "abc"), (8, 8, "bbc"), (9, 9, "bcc"), (10, 10, "ccc")]

    def test_postgresql(self):
        db = _connect_postgresql()
        
        declare_models(postgresql, db, 'tests.models')
        try:
            db.cursor().execute("TRUNCATE t1 RESTART IDENTITY CASCADE")

            self._execute(db)
        finally:
            db.rollback()
            del sys.modules['tests.models'].__dict__["t1"]
            del sys.modules['tests.models'].__dict__["t2"]
            del sys.modules['tests.models'].__dict__["t3"]
            del sys.modules['tests.models'].__dict__["t4"]

    def test_mysql(self):
        db = _connect_mysql()
        
        declare_models(mysql, db, 'tests.models')
        try:
            db.begin()
            db.cursor().execute("DELETE FROM t1")
            db.cursor().execute("ALTER TABLE t1 AUTO_INCREMENT = 1")

            self._execute(db)
        finally:
            db.rollback()
            del sys.modules['tests.models'].__dict__["t1"]
            del sys.modules['tests.models'].__dict__["t2"]
            del sys.modules['tests.models'].__dict__["t3"]
            del sys.modules['tests.models'].__dict__["t4"]
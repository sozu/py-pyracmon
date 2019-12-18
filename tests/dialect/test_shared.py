import pytest
from pyracmon.model import *
from pyracmon.mixin import CRUDMixin
from pyracmon.dialect.shared import *
from tests.db_api import *


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

class TestMultiInasert:
    def _db(self):
        return PseudoAPI().connect()

    def test_empty(self):
        db = self._db()
        MultiInsertMixin.inserts(db, [])

    def test_multi_insert(self):
        db = self._db()
        m = define_model(table1, [MultiInsertMixin, CRUDMixin])

        models = [m(c2 = i, c3 = f"v{i}") for i in range(0, 10)]

        m.inserts(db, models, dict(c2 = lambda h: f"{h} * 2"), 3)

        for i in range(0, 4):
            if i < 3:
                assert db.query_list[i] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?), (? * 2, ?), (? * 2, ?)"
                assert db.params_list[i] == [i * 3, f"v{i * 3}", i * 3 + 1, f"v{i * 3 + 1}", i * 3 + 2, f"v{i * 3 + 2}",]
            else:
                assert db.query_list[i] == "INSERT INTO t1 (c2, c3) VALUES (? * 2, ?)"
                assert db.params_list[i] == [9, f"v9"]

    def test_assign_pk(self):
        db = self._db()
        class LastSequences:
            seq = 90
            plus = [3, 3, 3, 1]
            @classmethod
            def last_sequences(cls, db, num):
                cls.seq += cls.plus.pop(0)
                return [(table1.columns[0], cls.seq)]
        m = define_model(table1, [LastSequences, MultiInsertMixin, CRUDMixin])

        models = [m(c2 = i, c3 = f"v{i}") for i in range(0, 10)]

        m.inserts(db, models, dict(c2 = lambda h: f"{h} * 2"), 3)

        assert [v.c1 for v in models] == [91, 92, 93, 94, 95, 96, 97, 98, 99, 100]

    def test_assign_pks(self):
        db = self._db()
        class LastSequences:
            seq1 = 90
            seq2 = 50
            plus = [3, 3, 3, 1]
            @classmethod
            def last_sequences(cls, db, num):
                p = cls.plus.pop(0)
                cls.seq1 += p
                cls.seq2 += p
                return [(table2.columns[0], cls.seq1), (table2.columns[1], cls.seq2)]
        m = define_model(table2, [LastSequences, MultiInsertMixin, CRUDMixin])

        models = [m(c3 = f"v{i}") for i in range(0, 10)]

        m.inserts(db, models, rows_per_insert = 3)

        assert [v.c1 for v in models] == [91, 92, 93, 94, 95, 96, 97, 98, 99, 100]
        assert [v.c2 for v in models] == [51, 52, 53, 54, 55, 56, 57, 58, 59, 60]
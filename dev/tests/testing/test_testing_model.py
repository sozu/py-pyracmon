import pytest
import psycopg2
from datetime import date, datetime, time, timedelta
from enum import Enum, auto
from uuid import uuid1, uuid3
from tests import models as m
from pyracmon import *
from pyracmon.dialect import postgresql
from pyracmon.testing.model import *
from pyracmon.testing.util import Near, one_of, default_test_config


def _connect():
    return connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )


class TestTruncate:
    def test_truncate(self):
        db = _connect()

        declare_models(postgresql, db, 'tests.models')

        m.t1.inserts(db, [m.t1(c12=i, c13=f"a{i}") for i in range(5)])
        m.t2.inserts(db, [m.t2(c21=i, c22=i, c23=f"a{i}") for i in range(5)])

        assert m.t1.count(db) == 5
        assert m.t2.count(db) == 5

        truncate(db, m.t1, m.t2)

        assert m.t1.count(db) == 0
        assert m.t2.count(db) == 0

        assert m.t1.insert(db, m.t1(c12=0, c13="")).c11 == 1


class TestMatch:
    def test_match(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        actual = m.types(
            bool_ = True,
            double_ = 0.1,
            int_ = 1,
            string_ = "abc",
            bytes_ = b"abc",
            date_ = date(2020, 1, 1),
            datetime_ = datetime(2020, 1, 1, 1, 1, 1),
            time_ = time(1, 2, 3),
            delta_ = timedelta(hours=1),
        )

        assert actual.match(
            bool_ = True,
            double_ = 0.1,
            int_ = 1,
            string_ = "abc",
            bytes_ = b"abc",
            date_ = date(2020, 1, 1),
            datetime_ = datetime(2020, 1, 1, 1, 1, 1),
            time_ = time(1, 2, 3),
            delta_ = timedelta(hours=1),
        )

        assert not actual.match(
            bool_ = False,
        )

        assert actual.match(
            bool_ = True,
            int_ = 1,
            bytes_ = b"abc",
            datetime_ = datetime(2020, 1, 1, 1, 1, 1),
            delta_ = timedelta(hours=1),
        )

        assert actual.match(
            double_ = Near(0.05, 0, 0.05),
            int_ = Near(2, -2),
            datetime_ = Near(datetime(2020, 1, 1, 1, 0, 31), 0, 1, seconds=30),
        )

        with pytest.raises(AttributeError):
            actual.match(unknown = None)


class TestFixture:
    def test_number(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        truncate(db, m.types)

        with default_test_config() as cfg:
            cfg.fixture_ignore_nullable = False

            today = date.today()
            now = datetime.now().astimezone()
            time = datetime.now().astimezone().time()

            models = m.types.by(1).fixture(db, 3, cfg=cfg)

            assert m.types.count(db) == 3

            for i,v in enumerate(models):
                assert v.match(
                    bool_ = True,
                    double_ = (1.2, 2.3, 3.4)[i],
                    int_ = (1, 2, 3)[i],
                    string_ = f"string_-{i+1}",
                    bytes_ = f"bytes_-{i+1}".encode(),
                    date_ = Near(today, 0, 1, days=1),
                    datetime_ = Near(now, 0, 10, seconds=1),
                    time_ = ~one_of(None),
                    delta_ = timedelta(days=(2, 3, 4)[i]),
                    uuid_ = str(uuid3(fixed_uuid, f"types-uuid_-{i+1}")),
                    enum_ = None,
                    record_ = None,
                    array_ = [],
                    deeparray_ = [],
                )

    def test_values(self):
        class E(Enum):
            E1 = auto()
            E2 = auto()

        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        with default_test_config() as cfg:
            cfg.fixture_ignore_nullable = False

            today = date.today()
            now = datetime.now().astimezone()

            m.types.column.enum_.ptype = E

            assert m.types.fixture(None, cfg=cfg)[0].match(
                bool_ = True,
                double_ = 1.2,
                int_ = 1,
                string_ = "string_-1",
                bytes_ = "bytes_-1".encode(),
                date_ = Near(today, 0, 1, days=1),
                datetime_ = Near(now, 0, 10, seconds=1),
                time_ = ~one_of(None),
                delta_ = timedelta(days=2),
                uuid_ = str(uuid3(fixed_uuid, f"types-uuid_-1")),
                enum_ = E.E1,
                record_ = None,
                array_ = [],
                deeparray_ = [],
            )

    def test_nullable(self):
        class E(Enum):
            E1 = auto()
            E2 = auto()

        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        m.types.column.enum_.ptype = E

        assert m.types.fixture(None)[0].match(
            bool_ = None,
            double_ = None,
            int_ = None,
            string_ = None,
            bytes_ = None,
            date_ = None,
            datetime_ = None,
            time_ = None,
            delta_ = None,
            uuid_ = None,
            enum_ = None,
            record_ = None,
            array_ = None,
            deeparray_ = None,
        )

    def test_model(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        truncate(db, m.t1)

        model = m.t1.by(1).fixture(db, m.t1(c12=12, c13="abc"))

        assert m.t1.count(db) == 1
        rec = m.t1.fetch(db, 1)
        assert rec and rec.match(c11=1, c12=12, c13="abc")

        model = m.t1.fixture(db, m.t1())

        assert m.t1.count(db) == 2
        rec = m.t1.fetch(db, 2)
        assert rec and rec.match(c11=2, c12=2, c13="c13-2")

    def test_dict(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        truncate(db, m.t1)

        model = m.t1.by(1).fixture(db, dict(c12=12, c13="abc"))

        assert m.t1.count(db) == 1
        rec = m.t1.fetch(db, 1)
        assert rec and rec.match(c11=1, c12=12, c13="abc")

        model = m.t1.fixture(db, dict())

        assert m.t1.count(db) == 2
        rec = m.t1.fetch(db, 2)
        assert rec and rec.match(c11=2, c12=2, c13="c13-2")

    def test_models(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        truncate(db, m.t1)

        models = m.t1.by(1).fixture(db, [m.t1(
            c12=10*(i+1), c13=f"a{i+1}",
        ) for i in range(3)])

        assert m.t1.count(db) == 3
        for i,v in enumerate(m.t1.fetch_where(db, orders=dict(c11=True))):
            assert v.match(c11=i+1, c12=10*(i+1), c13=f"a{i+1}")

        models = m.t1.fixture(db, [m.t1() for i in range(3)])

        assert m.t1.count(db) == 6
        assert m.t1.count(db, Q.ge(c11=4)) == 3
        for i,v in enumerate(m.t1.fetch_where(db, Q.ge(c11=4), orders=dict(c11=True))):
            assert v.match(c11=4+i, c12=4+i, c13=f"c13-{4+i}")

    def test_dicts(self):
        db = _connect()
        declare_models(postgresql, db, 'tests.models', mixins=[TestingMixin])

        truncate(db, m.t1)

        models = m.t1.by(1).fixture(db, [dict(
            c12=10*(i+1), c13=f"a{i+1}",
        ) for i in range(3)])

        assert m.t1.count(db) == 3
        for i,v in enumerate(m.t1.fetch_where(db, orders=dict(c11=True))):
            assert v.match(c11=i+1, c12=10*(i+1), c13=f"a{i+1}")

        models = m.t1.fixture(db, [dict() for i in range(3)])

        assert m.t1.count(db) == 6
        assert m.t1.count(db, Q.ge(c11=4)) == 3
        for i,v in enumerate(m.t1.fetch_where(db, Q.ge(c11=4), orders=dict(c11=True))):
            assert v.match(c11=4+i, c12=4+i, c13=f"c13-{4+i}")
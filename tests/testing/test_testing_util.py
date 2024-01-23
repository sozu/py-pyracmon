import pytest
import psycopg2
from datetime import date, datetime, time, timedelta
from tests import models as m
from pyracmon import *
from pyracmon.dialect import postgresql
from pyracmon.testing.util import *


class TestNear:
    def test_string(self):
        n = Near("abc")

        assert n.match("abc")
        assert not n.match("ab")

    def test_int_margin(self):
        n = Near(5)
        n01 = Near(5, None, 1)
        n10 = Near(5, -1)
        n11 = Near(5, -1, 1)

        assert n.match(5) and (not n.match(4)) and (not n.match(6))
        assert n01.match(5) and (not n01.match(4)) and n01.match(6) and (not n01.match(7))
        assert n10.match(5) and (not n10.match(3)) and n10.match(4) and (not n10.match(6))
        assert n11.match(5) and (not n11.match(3)) and n11.match(4) and n11.match(6) and (not n11.match(7))

    def test_datetime_margin(self):
        exp = datetime(2020, 1, 1, 0, 0, 0)

        n = Near(exp, -1, 1, minutes=1)

        assert not n.match(datetime(2019, 12, 31, 23, 58, 59))
        assert n.match(datetime(2019, 12, 31, 23, 59, 0))
        assert n.match(datetime(2020, 1, 1, 0, 0, 0))
        assert n.match(datetime(2020, 1, 1, 0, 1, 0))
        assert not n.match(datetime(2020, 1, 1, 0, 1, 1))

    def test_date_margin(self):
        exp = date(2020, 1, 1)

        n = Near(exp, -1, 1, days=2)

        assert not n.match(date(2019, 12, 29))
        assert n.match(date(2019, 12, 30))
        assert n.match(date(2020, 1, 1))
        assert n.match(date(2020, 1, 3))
        assert not n.match(date(2020, 1, 4))

    def test_configured(self):
        exp = datetime(2020, 1, 1, 0, 0, 0)

        n = near(exp, -1, 1)

        assert not n.match(datetime(2019, 12, 31, 23, 59, 58))
        assert n.match(datetime(2019, 12, 31, 23, 59, 59))
        assert n.match(datetime(2020, 1, 1, 0, 0, 0))
        assert n.match(datetime(2020, 1, 1, 0, 0, 1))
        assert not n.match(datetime(2020, 1, 1, 0, 0, 2))

    def test_update_config(self):
        exp = datetime(2020, 1, 1, 0, 0, 0)

        with default_test_config() as cfg:
            cfg.timedelta_unit = dict(minutes=1)

            n = near(exp, -1, 1)

            assert not n.match(datetime(2019, 12, 31, 23, 58, 59))
            assert n.match(datetime(2019, 12, 31, 23, 59, 0))
            assert n.match(datetime(2020, 1, 1, 0, 0, 0))
            assert n.match(datetime(2020, 1, 1, 0, 1, 0))
            assert not n.match(datetime(2020, 1, 1, 0, 1, 1))


class TestLet:
    def test_let(self):
        matcher = let(lambda x: x < 2)

        assert matcher.match(1)
        assert not matcher.match(2)


class TestOneOf:
    def test_one_of(self):
        matcher = one_of(1, 2, 3)

        assert matcher.match(1)
        assert not matcher.match(10)

    def test_none_of(self):
        matcher = ~one_of(1, 2, 3)

        assert not matcher.match(1)
        assert matcher.match(10)

    def test_with_near(self):
        matcher = one_of(1, Near(10, -1), 2, 3)

        assert matcher.match(1)
        assert matcher.match(9)
        assert matcher.match(10)
        assert not matcher.match(11)


class TestOperators:
    def test_and(self):
        m1 = let(lambda x: x > 0)
        m2 = let(lambda x: x < 3)

        matcher = m1 & m2

        assert not matcher.match(0)
        assert matcher.match(1)
        assert not matcher.match(3)

    def test_or(self):
        m1 = let(lambda x: x <= 0)
        m2 = let(lambda x: x >= 3)

        matcher = m1 | m2

        assert matcher.match(0)
        assert not matcher.match(1)
        assert matcher.match(3)

    def test_not(self):
        matcher = ~let(lambda x: x < 2)

        assert not matcher.match(1)
        assert matcher.match(2)

    def test_composite(self):
        m1 = let(lambda x: x > 0)
        m2 = let(lambda x: x < 3)
        m3 = let(lambda x: x%2 == 0)

        matcher = ~(m1 & m2) | m3

        assert matcher.match(5)
        assert not matcher.match(1)
        assert matcher.match(2)
        assert matcher.match(4)
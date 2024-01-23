import pytest
from pyracmon.sql import *
from pyracmon.marker import Marker
from tests.db_api import PseudoAPI


class TestSql:
    def test_q(self):
        sql, params = Sql(Marker.of("qmark"), f"$_1 $_ $a $_ $_3 $c $a $_ $_4").render(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10)
        assert sql == "? ? ? ? ? ? ? ? ?"
        assert params == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_numeric(self):
        sql, params = Sql(Marker.of("numeric"), f"$_1 $_ $a $_ $_3 $c $a $_ $_4").render(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10)
        assert sql == ":1 :2 :3 :4 :5 :6 :7 :8 :9"
        assert params == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_named(self):
        sql, params = Sql(Marker.of("named"), f"$_1 $_ $a $_ $_3 $c $a $_ $_4").render(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10)
        assert sql == ":param1 :param1 :a :param2 :param3 :c :a :param3 :param4"
        assert params == {"param1":1, "param2":2, "param3":3, "param4":4, "a":6, "c":8}

    def test_format(self):
        sql, params = Sql(Marker.of("format"), f"$_1 $_ $a $_ $_3 $c $a $_ $_4").render(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10)
        assert sql == "%s %s %s %s %s %s %s %s %s"
        assert params == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_pyformat(self):
        sql, params = Sql(Marker.of("pyformat"), f"$_1 $_ $a $_ $_3 $c $a $_ $_4").render(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10)
        assert sql == "%(param1)s %(param1)s %(a)s %(param2)s %(param3)s %(c)s %(a)s %(param3)s %(param4)s"
        assert params == {"param1":1, "param2":2, "param3":3, "param4":4, "a":6, "c":8}
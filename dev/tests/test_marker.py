import pytest
from pyracmon.marker import *


class TestMarkerOf:
    def test_of(self):
        assert isinstance(Marker.of('qmark'), QMarker)
        assert isinstance(Marker.of('numeric'), NumericMarker)
        assert isinstance(Marker.of('named'), NamedMarker)
        assert isinstance(Marker.of('format'), FormatMarker)
        assert isinstance(Marker.of('pyformat'), PyformatMarker)
        with pytest.raises(ValueError):
            Marker.of('unknown')


class TestQMarker:
    def test_no_arg(self):
        m = QMarker()
        assert [m(), m(), m()] == ['?'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3]

    def test_index(self):
        m = QMarker()
        assert [m(1), m(4), m(2)] == ['?'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 4, 2]

    def test_key(self):
        m = QMarker()
        assert [m("a"), m("b"), m("c")] == ['?'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [6, 7, 8]

    def test_mixed(self):
        m = QMarker()
        assert [m(1), m(), m("a"), m(), m(3), m("c"), m("a"), m(), m(4)] == ['?'] * 9
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_reset(self):
        m = QMarker()
        assert [m(), m()] == ['?', '?']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]
        assert [m(), m()] == ['?', '?']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3, 4]
        m.reset()
        assert [m(), m()] == ['?', '?']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]


class TestNumericMarker:
    def test_no_arg(self):
        m = NumericMarker()
        assert [m(), m(), m()] == [":1", ":2", ":3"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3]

    def test_index(self):
        m = NumericMarker()
        assert [m(1), m(4), m(2)] == [":1", ":2", ":3"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 4, 2]

    def test_key(self):
        m = NumericMarker()
        assert [m("a"), m("b"), m("c")] == [":1", ":2", ":3"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [6, 7, 8]

    def test_mixed(self):
        m = NumericMarker()
        assert [m(1), m(), m("a"), m(), m(3), m("c"), m("a"), m(), m(4)] \
            == [":1", ":2", ":3", ":4", ":5", ":6", ":7", ":8", ":9"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_reset(self):
        m = NumericMarker()
        assert [m(), m()] == [":1", ":2"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]
        assert [m(), m()] == [":3", ":4"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3, 4]
        m.reset()
        assert [m(), m()] == [":1", ":2"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]


class TestNamedMarker:
    def test_no_arg(self):
        m = NamedMarker()
        assert [m(), m(), m()] == [":param1", ":param2", ":param3"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3}

    def test_index(self):
        m = NamedMarker()
        assert [m(1), m(4), m(2)] == [":param1", ":param4", ":param2"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param4":4, "param2":2}

    def test_key(self):
        m = NamedMarker()
        assert [m("a"), m("b"), m("c")] == [":a", ":b", ":c"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"a":6, "b":7, "c":8}

    def test_mixed(self):
        m = NamedMarker()
        assert [m(1), m(), m("a"), m(), m(3), m("c"), m("a"), m(), m(4)] \
            == [":param1", ":param1", ":a", ":param2", ":param3", ":c", ":a", ":param3", ":param4"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3, "param4":4, "a":6, "c":8}

    def test_reset(self):
        m = NamedMarker()
        assert [m(), m()] == [":param1", ":param2"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2}
        assert [m(), m()] == [":param3", ":param4"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3, "param4":4}
        m.reset()
        assert [m(), m()] == [":param1", ":param2"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2}


class TestFormatMarker:
    def test_no_arg(self):
        m = FormatMarker()
        assert [m(), m(), m()] == ['%s'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3]

    def test_index(self):
        m = FormatMarker()
        assert [m(1), m(4), m(2)] == ['%s'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 4, 2]

    def test_key(self):
        m = FormatMarker()
        assert [m("a"), m("b"), m("c")] == ['%s'] * 3
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [6, 7, 8]

    def test_mixed(self):
        m = FormatMarker()
        assert [m(1), m(), m("a"), m(), m(3), m("c"), m("a"), m(), m(4)] == ['%s'] * 9
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 1, 6, 2, 3, 8, 6, 3, 4]

    def test_reset(self):
        m = FormatMarker()
        assert [m(), m()] == ['%s', '%s']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]
        assert [m(), m()] == ['%s', '%s']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2, 3, 4]
        m.reset()
        assert [m(), m()] == ['%s', '%s']
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == [1, 2]


class TestPyformatMarker:
    def test_no_arg(self):
        m = PyformatMarker()
        assert [m(), m(), m()] == ["%(param1)s", "%(param2)s", "%(param3)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3}

    def test_index(self):
        m = PyformatMarker()
        assert [m(1), m(4), m(2)] == ["%(param1)s", "%(param4)s", "%(param2)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param4":4, "param2":2}

    def test_key(self):
        m = PyformatMarker()
        assert [m("a"), m("b"), m("c")] == ["%(a)s", "%(b)s", "%(c)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"a":6, "b":7, "c":8}

    def test_mixed(self):
        m = PyformatMarker()
        assert [m(1), m(), m("a"), m(), m(3), m("c"), m("a"), m(), m(4)] \
            == ["%(param1)s", "%(param1)s", "%(a)s", "%(param2)s", "%(param3)s", "%(c)s", "%(a)s", "%(param3)s", "%(param4)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3, "param4":4, "a":6, "c":8}

    def test_reset(self):
        m = PyformatMarker()
        assert [m(), m()] == ["%(param1)s", "%(param2)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2}
        assert [m(), m()] == ["%(param3)s", "%(param4)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2, "param3":3, "param4":4}
        m.reset()
        assert [m(), m()] == ["%(param1)s", "%(param2)s"]
        assert m.params(1, 2, 3, 4, 5, a=6, b=7, c=8, d=9, e=10) == {"param1":1, "param2":2}


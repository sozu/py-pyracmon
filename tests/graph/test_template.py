import pytest
from pyracmon.graph.template import *


class TestGraphTemplate:
    def _template(self):
        return graph_template(
            a = (),
            b = (int,),
            c = int,
            d = (str, len)
        )

    def test_define(self):
        t = self._template()

        assert len(t._properties) == 4
        assert len(t._relations) == 0

        assert (t.a.name, t.a.kind, t.a.identifier) == ("a", None, None)
        assert (t.b.name, t.b.kind, t.b.identifier) == ("b", int, None)
        assert (t.c.name, t.c.kind, t.c.identifier) == ("c", int, None)
        assert (t.d.name, t.d.kind, t.d.identifier) == ("d", str, len)

    def test_rshift(self):
        t = self._template()

        t.a >> t.b
        assert t._relations == [(t.a, t.b)]
        assert t.a.parent == t.b
        assert t.b.children == [t.a]

    def test_lshift(self):
        t = self._template()

        t.c << t.b
        assert t._relations == [(t.b, t.c)]
        assert t.b.parent == t.c
        assert t.c.children == [t.b]

    def test_multi_lshift(self):
        t = self._template()

        t.c << [t.a, t.b]
        assert t._relations == [(t.a, t.c), (t.b, t.c)]
        assert t.a.parent == t.c
        assert t.b.parent == t.c
        assert t.c.children == [t.a, t.b]


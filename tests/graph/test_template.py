import pytest
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import *

spec = GraphSpec()

class TestGraphTemplate:
    def _template(self):
        return spec.new_template(
            a = (),
            b = (int,),
            c = int,
            d = (str, len)
        )

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


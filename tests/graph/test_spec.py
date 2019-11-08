import pytest
from pyracmon.graph.spec import GraphSpec


class TestIdentifier:
    def test_find(self):
        identi = lambda x: x
        idents = lambda x: x

        spec = GraphSpec()
        spec.add_identifier(int, identi)
        spec.add_identifier(str, idents)

        assert spec.get_identifier(int) is identi
        assert spec.get_identifier(str) is idents
        assert spec.get_identifier(float) is None

    def test_inherit(self):
        class A:
            pass
        class B(A):
            pass
        ident = lambda x: x

        spec = GraphSpec()
        spec.add_identifier(A, ident)

        assert spec.get_identifier(B) is ident


class TestNewTemplate:
    def test_default(self):
        t = GraphSpec().new_template(
            a = (),
            b = (int,),
            c = int,
            d = (str, len)
        )

        assert len(t._properties) == 4
        assert len(t._relations) == 0

        assert (t.a.name, t.a.kind, t.a.identifier) == ("a", None, None)
        assert (t.b.name, t.b.kind, t.b.identifier) == ("b", int, None)
        assert (t.c.name, t.c.kind, t.c.identifier) == ("c", int, None)
        assert (t.d.name, t.d.kind, t.d.identifier) == ("d", str, len)

    def test_identifier(self):
        ident1 = lambda x: x * 1
        ident2 = lambda x: x * 2

        spec = GraphSpec()
        spec.add_identifier(int, ident1)

        t = spec.new_template(
            a = int,
            b = (int,),
            c = (int, None),
            d = (int, ident2),
        )

        assert (t.a.name, t.a.kind, t.a.identifier) == ("a", int, ident1)
        assert (t.b.name, t.b.kind, t.b.identifier) == ("b", int, ident1)
        assert (t.c.name, t.c.kind, t.c.identifier) == ("c", int, None)
        assert (t.d.name, t.d.kind, t.d.identifier) == ("d", int, ident2)
import pytest
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.graph import IdentifyPolicy


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


class TestPropertyDefinition:
    ident = lambda x:x
    ef = lambda x:True

    def _spec(self):
        spec = GraphSpec()
        spec.add_identifier(int, self.ident)
        spec.add_entity_filter(int, self.ef)
        return spec

    def test_none(self):
        spec = self._spec()
        kind, ident, ef = spec.get_property_definition(None)
        assert kind is None
        assert ident(None) is None and ident(None, None) == (False, [], [])
        assert ef is None

    def test_tuple_0(self):
        spec = self._spec()
        kind, ident, ef = spec.get_property_definition(())
        assert kind is None
        assert ident(None) is None and ident(None, None) == (False, [], [])
        assert ef is None

    def test_tuple_1(self):
        spec = self._spec()
        kind, ident, ef = spec.get_property_definition((int))
        assert kind is int
        assert ident.identifier is self.ident
        assert ef is self.ef

    def test_tuple_2(self):
        spec = self._spec()
        ident = lambda x:x
        kind, ident, ef = spec.get_property_definition((int, ident))
        assert kind is int
        assert ident.identifier is ident
        assert ef is self.ef

    def test_tuple_3(self):
        spec = self._spec()
        ident = lambda x:x
        ef = lambda x:True
        kind, ident, ef = spec.get_property_definition((int, ident, ef))
        assert kind is int
        assert ident.identifier is ident
        assert ef is ef

    def test_policy(self):
        spec = self._spec()
        policy = IdentifyPolicy.never()
        kind, ident, ef = spec.get_property_definition((int, policy))
        assert kind is int
        assert ident.identifier is policy
        assert ef is self.ef

    def test_type(self):
        spec = self._spec()
        kind, ident, ef = spec.get_property_definition(int)
        assert kind is int
        assert ident.identifier is self.ident
        assert ef is self.ef


class TestNewTemplate:
    def test_default(self):
        t = GraphSpec().new_template(
            a = (),
            b = (int,),
            c = int,
            d = (str, len, lambda s: len(s) > 3)
        )

        assert len(t._properties) == 4
        assert len(t._relations) == 0

        assert (t.a.name, t.a.kind, t.a.identifier.identifier("abc")) == ("a", None, None)
        assert (t.b.name, t.b.kind, t.b.identifier.identifier("abc")) == ("b", int, None)
        assert (t.c.name, t.c.kind, t.c.identifier.identifier("abc")) == ("c", int, None)
        assert (t.d.name, t.d.kind, t.d.identifier.identifier("abc")) == ("d", str, 3)
        assert (t.a.entity_filter, t.b.entity_filter, t.c.entity_filter) == (None, None, None)
        assert t.d.entity_filter("abc") == False
        assert t.d.entity_filter("abcd") == True

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

        assert (t.a.name, t.a.kind, t.a.identifier.identifier(4)) == ("a", int, 4)
        assert (t.b.name, t.b.kind, t.b.identifier.identifier(4)) == ("b", int, 4)
        assert (t.c.name, t.c.kind, t.c.identifier.identifier(4)) == ("c", int, None)
        assert (t.d.name, t.d.kind, t.d.identifier.identifier(4)) == ("d", int, 8)


class TestMergeTemplate:
    def test_merge(self):
        id1 = lambda x:x*2
        ef1 = lambda x:x%2==0

        t1 = spec.new_template(
            a = (int, id1, ef1),
        )
        t2 = spec.new_template(
            t1,
            b = str,
        )

        assert t2._properties == [t2.b, t2.a]
        assert t2.a.kind is int
        assert t2.a.identifier.identifier(5) == 10
        assert t2.a.entity_filter(2) and not t2.a.entity_filter(3)

    def test_merge_relation(self):
        t1 = spec.new_template(
            a = int,
            b = int,
            c = int,
            d = int,
        )
        t1.a << [t1.b, t1.d >> t1.c]

        t2 = spec.new_template(
            t1,
            e = str,
        )

        assert t2._properties == [t2.e, t2.a, t2.b, t2.c, t2.d]
        assert t2.a.parent is None
        assert t2.b.parent is t2.a
        assert t2.c.parent is t2.a
        assert t2.d.parent is t2.c
import pytest
from pyracmon.graph.template import *
from pyracmon.graph.identify import IdentifyPolicy


class TestCreateGraphTemplate:
    def test_empty(self):
        t = GraphTemplate([])

        assert t._properties == {}
        assert t._relations == []

    def test_create(self):
        policy = IdentifyPolicy(lambda x:x)
        ef = lambda x:True

        t = GraphTemplate([
            ("a", int, policy, ef),
            ("b", str, policy, ef),
        ])

        assert isinstance(t.a, GraphTemplate.Property)
        assert (t.a.template, t.a.name, t.a.kind, t.a.policy, t.a.entity_filter) \
            == (t, "a", int, policy, ef)
        assert isinstance(t.b, GraphTemplate.Property)
        assert (t.b.template, t.b.name, t.b.kind, t.b.policy, t.b.entity_filter) \
            == (t, "b", str, policy, ef)

    def test_fail_name_duplicate(self):
        with pytest.raises(ValueError):
            t = GraphTemplate([
                ("b", int, None, None),
                ("b", str, None, None),
            ])


class TestCopiedProperty:
    def test_copy_property(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
        ])
        t.a << t.b << t.c

        u = GraphTemplate([
            ("d", t.b, None, None),
            ("e", int, None, None),
            ("f", int, None, None),
        ])
        u.f << u.d
        u.c << u.e

        assert list(u) == [u.f, u.d, u.c, u.e]
        assert u.d.parent is u.f
        assert u.c.parent is u.d
        assert u.e.parent is u.c

    def test_copy_template(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
        ])
        t.a << t.b << t.c

        u = GraphTemplate([
            ("d", t, None, None),
            ("e", int, None, None),
            ("f", int, None, None),
        ])
        u.f << u.d

        assert list(u) == [u.e, u.f, u.d]
        assert u.d.parent is u.f
        assert u.e.parent is None


class TestShift:
    def _template(self):
        return GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
        ])

    def test_lshift(self):
        t = self._template()
        r = t.a << t.b << t.c

        assert r is t.c
        assert t.a.parent is None
        assert t.b.parent is t.a
        assert t.c.parent is t.b
        assert t.a.children == [t.b]
        assert t.b.children == [t.c]
        assert t.c.children == []

    def test_multi_lshift(self):
        t = self._template()
        r = t.a << [t.b, t.c]

        assert r == [t.b, t.c]
        assert t.a.parent is None
        assert t.b.parent is t.a
        assert t.c.parent is t.a
        assert t.a.children == [t.b, t.c]
        assert t.b.children == []
        assert t.c.children == []

    def test_rshift(self):
        t = self._template()
        r = t.a >> t.b >> t.c

        assert r is t.c
        assert t.a.parent is t.b
        assert t.b.parent is t.c
        assert t.c.parent is None
        assert t.a.children == []
        assert t.b.children == [t.a]
        assert t.c.children == [t.b]

    def test_multi_rshift(self):
        t = self._template()
        r = [t.a, t.b] >> t.c

        assert r is t.c
        assert t.a.parent is t.c
        assert t.b.parent is t.c
        assert t.c.parent is None
        assert t.a.children == []
        assert t.b.children == []
        assert t.c.children == [t.a, t.b]

    def test_fail_multi_parent(self):
        t = self._template()
        t.a >> t.b
        with pytest.raises(ValueError):
            t.a >> t.c

    def test_fail_recursive(self):
        t = self._template()
        t.a >> t.b >> t.c
        with pytest.raises(ValueError):
            t.c >> t.a

    def test_fail_another_template(self):
        t1 = self._template()
        t2 = self._template()
        with pytest.raises(ValueError):
            t1.a >> t2.b

    def test_fail_graph_property(self):
        t = self._template()
        u = GraphTemplate([
            ("t", t, None, None),
            ("d", int, None, None),
        ])
        with pytest.raises(ValueError):
            u.d >> u.t


class TestIterProperties:
    def _template(self):
        return GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])

    def test_iter(self):
        t = self._template()
        assert list(t) == [t.a, t.b, t.c, t.d]

    def test_hierarchy1(self):
        t = self._template()
        t.a << t.c << t.b
        assert list(t) == [t.a, t.c, t.b, t.d]

    def test_hierarchy2(self):
        t = self._template()
        t.a << [t.c, t.b]
        t.d << t.a
        assert list(t) == [t.d, t.a, t.c, t.b]


class TestMergeTemplate:
    def _template(self, index):
        return GraphTemplate([
            (f"a{index}", int, None, None),
            (f"b{index}", int, None, None),
            (f"c{index}", int, None, None),
        ])

    def test_iadd(self):
        t1 = self._template(1)
        t2 = self._template(2)

        t1.a1 << [t1.b1, t1.c1]
        t2.b2 << t2.c2

        t1 += t2

        t1.b1 << t1.b2
        t1.a2 << t1.a1

        assert list(t1) == [t1.a2, t1.a1, t1.b1, t1.b2, t1.c2, t1.c1]
        assert t1.a1.parent == t1.a2
        assert t1.a2.children == [t1.a1]
        assert t1.b2.parent == t1.b1
        assert t1.b1.children == [t1.b2]
        assert t1.c2.parent == t1.b2
        assert t1.b2.children == [t1.c2]

    def test_add(self):
        t1 = self._template(1)
        t2 = self._template(2)

        t1.a1 << [t1.b1, t1.c1]
        t2.b2 << t2.c2

        t = t1 + t2

        t.b1 << t.b2
        t.a2 << t.a1

        assert list(t) == [t.a2, t.a1, t.b1, t.b2, t.c2, t.c1]
        assert t.a1.parent == t.a2
        assert t.a2.children == [t.a1]
        assert t.b2.parent == t.b1
        assert t.b1.children == [t.b2]
        assert t.c2.parent == t.b2
        assert t.b2.children == [t.c2]
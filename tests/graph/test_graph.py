import pytest
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.identify import *
from pyracmon.graph.graph import *


class TestNode:
    def _template(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
        ])
        t.a << t.b << t.c
        return t

    def test_attributes(self):
        t = self._template()

        na = Node(t.a, 1, 1, 0)
        assert na.name == "a"
        assert set(na.children.keys()) == {"b"}

        nc = Node(t.c, 1, 1, 0)
        assert nc.name == "c"
        assert set(nc.children.keys()) == set()

    def test_add_child(self):
        t = self._template()
        na = Node(t.a, 1, 1, 0)
        nb = Node(t.b, 2, 2, 0)
        nc = Node(t.c, 3, 3, 0)

        na.add_child(nb)

        assert na.children["b"].nodes == [nb]
        assert nb in na.children["b"]
        assert nb.parents == {na}

    def test_fail_add_not_child(self):
        t = self._template()
        na = Node(t.a, 1, 1, 0)
        nc = Node(t.c, 3, 3, 0)

        with pytest.raises(KeyError):
            na.add_child(nc)

    def test_fail_add_different_template(self):
        t1 = self._template()
        na = Node(t1.a, 1, 1, 0)
        t2 = self._template()
        nb = Node(t2.b, 2, 2, 0)

        with pytest.raises(ValueError):
            na.add_child(nb)

    def test_has_child(self):
        t1 = self._template()
        na = Node(t1.a, 1, 1, 0)
        nb1 = Node(t1.b, 2, 2, 0)
        nb2 = Node(t1.b, 3, 3, 0)

        assert not na.has_child(nb1) and not na.has_child(nb2)
        na.add_child(nb1)
        assert na.has_child(nb1) and not na.has_child(nb2)


class TestNodeView:
    def _template(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_view(self):
        t = self._template()
        n = Node(t.a, 1, None, 0)
        for i in range(3):
            n.add_child(Node(t.b, 10+i, None, i))
        v = n.view

        assert v() == 1
        assert v.b is n.children["b"].view
        assert v.c is n.children["c"].view
        assert list(v) == [("b", n.children["b"].view), ("c", n.children["c"].view)]

        with pytest.raises(KeyError):
            v.d

    def test_children_view(self):
        t = self._template()
        n = Node(t.a, 1, None, 0)

        b1, b2, b3 = [Node(t.b, 10+i, None, i) for i in range(3)]
        n.add_child(b1).add_child(b2).add_child(b3)

        d1, d2 = [Node(t.d, 30+i, None, i) for i in range(2)]
        b1.add_child(d1).add_child(d2)

        vb = n.view.b
        vc = n.view.c

        assert bool(vb)
        assert vb() is n.children["b"]
        assert list(vb) == [b1.view, b2.view, b3.view]
        assert len(vb) == 3
        assert (vb[0], vb[1], vb[2]) == (b1.view, b2.view, b3.view)
        assert vb.d is vb[0].d

        assert not bool(vc)
        assert vc() is n.children["c"]
        assert list(vc) == []
        assert len(vc) == 0

        vd = vb.d
        assert bool(vd)
        assert list(vd) == [d1.view, d2.view]


class TestContainerView:
    def _template(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_empty(self):
        t = self._template()
        ca = NodeContainer(t.a)
        v = ca.view

        assert not bool(v)
        assert v() is ca
        assert len(v) == 0
        assert list(v) == []

    def test_view(self):
        t = self._template()
        ca = NodeContainer(t.a)
        n1, n2, n3 = [Node(t.a, i, i, i) for i in range(3)]
        ca.nodes.append(n1); ca.keys[0] = [0]
        ca.nodes.append(n2); ca.keys[1] = [1]
        ca.nodes.append(n3); ca.keys[2] = [2]
        b1, b2 = [Node(t.b, 10+i, 10+i, i) for i in range(2)]
        n1.add_child(b1); n1.add_child(b2)
        v = ca.view

        assert bool(v)
        assert v() is ca
        assert len(v) == 3
        assert list(v) == [n1.view, n2.view, n3.view]
        assert (v[0], v[1], v[2]) == (n1.view, n2.view, n3.view)
        assert v.b is n1.children["b"].view


class TestNodeContainer:
    def _template(self, policy):
        if policy == 'hierarchy':
            p = HierarchicalPolicy(lambda x:x)
        elif policy == 'always':
            p = AlwaysPolicy(lambda x:x)
        else:
            p = NeverPolicy(lambda x:x)

        t = GraphTemplate([
            ("a", int, p, None),
            ("b", int, p, None),
            ("c", int, p, None),
            ("d", int, p, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_attributes(self):
        t = self._template("hierarchy")
        container = NodeContainer(t.a)
        assert container.name == "a"

    def _prepare(self, policy) -> tuple[NodeContainer, NodeContainer, list[Node], list[Node]]:
        t = self._template(policy)
        ca = NodeContainer(t.a); cb = NodeContainer(t.b)
        nas = [Node(t.a, i, i, i) for i in range(3)]
        nbs = [Node(t.b, 10+(i%2), 10+(i%2), i) for i in range(6)]
        for i in range(3):
            nas[i].add_child(nbs[i*2]).add_child(nbs[i*2+1])
        for i, n in enumerate(nas):
            ca.nodes.append(n)
            ca.keys.setdefault(n.key, []).append(i)
        for i, n in enumerate(nbs):
            cb.nodes.append(n)
            cb.keys.setdefault(n.key, []).append(i)
        return ca, cb, nas, nbs

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_root_new(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {}
        ca.append(3, anc)

        assert len(ca.view) == 4
        assert [n() for n in ca.view] == [0, 1, 2, 3]
        assert anc == {"a": [ca.nodes[3]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_root_exists(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {}
        ca.append(1, anc)

        if policy in ("hierarchy", "always"):
            assert len(ca.view) == 3
            assert [n() for n in ca.view] == [0, 1, 2]
            assert anc == {"a": [nas[1]]}
        else:
            assert len(ca.view) == 4
            assert [n() for n in ca.view] == [0, 1, 2, 1]
            assert anc == {"a": [ca.nodes[3]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p0_new(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {}
        cb.append(12, anc)

        assert len(cb.view) == 7
        assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12]
        assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
        assert anc == {"b": [cb.nodes[6]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p0_exists(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {}
        cb.append(11, anc)

        if policy in ("hierarchy", "always"):
            assert len(cb.view) == 6
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"b": [nbs[1], nbs[3], nbs[5]]}
        else:
            assert len(cb.view) == 7
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"b": [cb.nodes[6]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p1_new(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {"a":[nas[1]]}
        cb.append(12, anc)

        assert len(cb.view) == 7
        assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12]
        assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11, 12]), (2, [10, 11])]
        assert anc == {"a":[nas[1]], "b": [cb.nodes[6]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p1_exists(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {"a":[nas[1]]}
        cb.append(11, anc)

        if policy in ("hierarchy",):
            assert len(cb.view) == 6
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"a":[nas[1]], "b": [nbs[3]]}
        elif policy in ("always",):
            assert len(cb.view) == 6
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"a":[nas[1]], "b": [nbs[1], nbs[3], nbs[5]]}
        else:
            assert len(cb.view) == 7
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11, 11]), (2, [10, 11])]
            assert anc == {"a":[nas[1]], "b": [cb.nodes[6]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p2_new(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {"a":[nas[0], nas[2]]}
        cb.append(12, anc)

        assert len(cb.view) == 8
        assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12, 12]
        assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11, 12]), (1, [10, 11]), (2, [10, 11, 12])]
        assert anc == {"a":[nas[0], nas[2]], "b": [cb.nodes[6], cb.nodes[7]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p2_exists(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        anc = {"a":[nas[0], nas[2]]}
        cb.append(11, anc)

        if policy in ("hierarchy",):
            assert len(cb.view) == 6
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"a":[nas[0], nas[2]], "b": [nbs[1], nbs[5]]}
        elif policy in ("always",):
            assert len(cb.view) == 6
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11]), (1, [10, 11]), (2, [10, 11])]
            assert anc == {"a":[nas[0], nas[2]], "b": [nbs[1], nbs[3], nbs[5]]}
        else:
            assert len(cb.view) == 8
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 11, 11]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11, 11]), (1, [10, 11]), (2, [10, 11, 11])]
            assert anc == {"a":[nas[0], nas[2]], "b": [cb.nodes[6], cb.nodes[7]]}

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_child_p2_partial(self, policy):
        ca, cb, nas, nbs = self._prepare(policy)

        nn = Node(cb.prop, 12, 12, 6)
        nas[0].add_child(nn)
        cb.nodes.append(nn)
        cb.keys[12] = [6]

        assert len(cb.view) == 7
        assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12]

        anc = {"a":[nas[0], nas[2]]}
        cb.append(12, anc)

        if policy in ("hierarchy",):
            assert len(cb.view) == 8
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12, 12]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11, 12]), (1, [10, 11]), (2, [10, 11, 12])]
            assert anc == {"a":[nas[0], nas[2]], "b": [cb.nodes[6], cb.nodes[7]]}
        elif policy in ("always",):
            assert len(cb.view) == 8
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12, 12]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11, 12]), (1, [10, 11]), (2, [10, 11, 12])]
            assert anc == {"a":[nas[0], nas[2]], "b": [cb.nodes[6], cb.nodes[7]]}
        else:
            assert len(cb.view) == 9
            assert [n() for n in cb.view] == [10, 11, 10, 11, 10, 11, 12, 12, 12]
            assert [(na(), [nb() for nb in na.b]) for na in ca.view] == [(0, [10, 11, 12, 12]), (1, [10, 11]), (2, [10, 11, 12])]
            assert anc == {"a":[nas[0], nas[2]], "b": [cb.nodes[7], cb.nodes[8]]}


class TestNodeContainerReplace:
    def _template(self):
        t = GraphTemplate([
            ("a", int, HierarchicalPolicy(lambda x:x%3), None),
            ("b", int, HierarchicalPolicy(lambda x:x%4), None),
        ])
        t.a << t.b
        return t

    def test_replace_root(self):
        c = NodeContainer(self._template().a)

        c.append(1, {}, False)
        c.append(2, {}, False)
        c.append(3, {}, False)

        assert [n.entity for n in c.nodes] == [1, 2, 3]

        c.append(4, {}, True)
        c.append(6, {}, True)

        assert [n.entity for n in c.nodes] == [4, 2, 6]

    def test_child_root(self):
        t = self._template()
        ca = NodeContainer(t.a)
        cb = NodeContainer(t.b)

        ca.append(1, {}, False)
        ca.append(2, {}, False)
        ca.append(3, {}, False)

        cb.append(0, {"a": [ca.nodes[0]]}, False)
        cb.append(1, {"a": [ca.nodes[0]]}, False)
        cb.append(2, {"a": [ca.nodes[0]]}, False)
        cb.append(3, {"a": [ca.nodes[0]]}, False)
        cb.append(3, {"a": [ca.nodes[1]]}, False)
        cb.append(3, {"a": [ca.nodes[2]]}, False)

        assert [n.entity for n in cb.nodes] == [0, 1, 2, 3, 3, 3]

        cb.append(4, {"a": [ca.nodes[0], ca.nodes[1]]}, True)
        cb.append(7, {"a": [ca.nodes[0], ca.nodes[2]]}, True)

        assert [(n.entity, next(iter(n.parents))) for n in cb.nodes] \
            == [(4, ca.nodes[0]), (1, ca.nodes[0]), (2, ca.nodes[0]), (7, ca.nodes[0]), (3, ca.nodes[1]), (7, ca.nodes[2]), (4, ca.nodes[1])]


class TestGraphView:
    def _template(self, policy="hierarchy"):
        if policy == 'hierarchy':
            p = HierarchicalPolicy(lambda x:x)
        elif policy == 'always':
            p = AlwaysPolicy(lambda x:x)
        else:
            p = NeverPolicy(lambda x:x)

        t = GraphTemplate([
            ("a", int, p, None),
            ("b", int, p, None),
            ("c", int, p, None),
            ("d", int, p, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_view(self):
        t = self._template()
        graph = Graph(t)

        for i in range(6):
            anc = {}
            graph.containers["a"].append(   (0, 1, 0, 1, 0, 1)[i], anc)
            graph.containers["b"].append(10+(0, 1, 0, 2, 0, 1)[i], anc)
            graph.containers["c"].append(20+(0, 1, 2, 3, 4, 5)[i], anc)
            graph.containers["d"].append(30+(0, 1, 2, 3, 4, 5)[i], anc)

        v = graph.view

        assert v() == graph
        assert list(v) == [("a", graph.containers["a"].view)]
        assert v.a == graph.containers["a"].view
        assert v.b == graph.containers["b"].view
        assert v.c == graph.containers["c"].view
        assert v.d == graph.containers["d"].view
        assert [na() for na in v.a] == [0, 1]
        assert [[nb() for nb in na.b] for na in v.a] == [[10], [11, 12]]
        assert [[nc() for nc in na.c] for na in v.a] == [[20, 22, 24], [21, 23, 25]]
        assert [[[nd() for nd in nb.d] for nb in na.b] for na in v.a] == [[[30, 32, 34]], [[31, 35], [33]]]


class TestGraph:
    def _template(self, policy="hierarchy"):
        if policy == 'hierarchy':
            p = HierarchicalPolicy(lambda x:x)
        elif policy == 'always':
            p = AlwaysPolicy(lambda x:x)
        else:
            p = NeverPolicy(lambda x:x)

        t = GraphTemplate([
            ("a", int, p, lambda x: x>=0),
            ("b", int, p, lambda x: x>=0),
            ("c", int, p, lambda x: x>=0),
            ("d", int, p, lambda x: x>=0),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append(self, policy):
        t = self._template(policy)
        graph = Graph(t)

        graph.append(a=0, b=10, c=20, d=30)
        graph.append(a=0, b=10, c=21, d=31)
        graph.append(a=0, b=11, c=21, d=30)
        graph.append(a=0, b=11, c=22, d=30)
        graph.append(a=0, b=12, c=22, d=30)
        graph.append(a=0, b=11, c=22, d=32)
        graph.append(a=1, b=10, c=20, d=30)
        graph.append(a=2, b=10, c=20, d=30)
        graph.append(a=2, b=10, c=21, d=31)

        v = graph.view

        if policy == "hierarchy":
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (3, 5, 6, 8)
            assert [n() for n in v.a] == [0, 1, 2]
            assert [n() for n in v.b] == [10, 11, 12, 10, 10]
            assert [n() for n in v.c] == [20, 21, 22, 20, 20, 21]
            assert [n() for n in v.d] == [30, 31, 30, 30, 32, 30, 30, 31]
            assert [[m() for m in n.b] for n in v.a] == [[10, 11, 12], [10], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21, 22], [20], [20, 21]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 31], [30, 32], [30]], [[30]], [[30, 31]]]
        elif policy == "always":
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (3, 5, 6, 9)
            assert [n() for n in v.a] == [0, 1, 2]
            assert [n() for n in v.b] == [10, 11, 12, 10, 10]
            assert [n() for n in v.c] == [20, 21, 22, 20, 20, 21]
            assert [n() for n in v.d] == [30, 31, 30, 30, 32, 30, 30, 31, 31]
            assert [[m() for m in n.b] for n in v.a] == [[10, 11, 12], [10], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21, 22], [20], [20, 21]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 31], [30, 32], [30]], [[30, 31]], [[30, 31]]]
        else:
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (9, 9, 9, 9)
            assert [n() for n in v.a] == [0, 0, 0, 0, 0, 0, 1, 2, 2]
            assert [n() for n in v.b] == [10, 10, 11, 11, 12, 11, 10, 10, 10]
            assert [n() for n in v.c] == [20, 21, 21, 22, 22, 22, 20, 20, 21]
            assert [n() for n in v.d] == [30, 31, 30, 30, 30, 32, 30, 30, 31]
            assert [[m() for m in n.b] for n in v.a] == [[10], [10], [11], [11], [12], [11], [10], [10], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20], [21], [21], [22], [22], [22], [20], [20], [21]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30]], [[31]], [[30]], [[30]], [[30]], [[32]], [[30]], [[30]], [[31]]]

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_append_intermediate(self, policy):
        t = self._template(policy)
        graph = Graph(t)

        graph.append(a=0, b=10, d=30)
        graph.append(a=0, b=11, d=31)
        graph.append(a=1, b=10, d=32)
        graph.append(a=1, b=11, d=31)
        graph.append(     b=10, d=30)
        graph.append(     b=10, d=31)
        graph.append(           d=32)
        graph.append(           d=33)

        v = graph.view

        if policy == "hierarchy":
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (2, 4, 0, 8)
            assert [n() for n in v.a] == [0, 1]
            assert [n() for n in v.b] == [10, 11, 10, 11]
            assert [n() for n in v.c] == []
            assert [n() for n in v.d] == [30, 31, 32, 31, 30, 31, 31, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10, 11], [10, 11]]
            assert [[m() for m in n.c] for n in v.a] == [[], []]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 31], [31]], [[32, 30, 31], [31]]]
        elif policy == "always":
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (2, 4, 0, 9)
            assert [n() for n in v.a] == [0, 1]
            assert [n() for n in v.b] == [10, 11, 10, 11]
            assert [n() for n in v.c] == []
            assert [n() for n in v.d] == [30, 31, 32, 32, 31, 30, 31, 31, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10, 11], [10, 11]]
            assert [[m() for m in n.c] for n in v.a] == [[], []]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 32, 31], [31]], [[32, 30, 31], [31]]]
        else:
            assert (len(v.a), len(v.b), len(v.c), len(v.d)) == (4, 6, 0, 8)
            assert [n() for n in v.a] == [0, 0, 1, 1]
            assert [n() for n in v.b] == [10, 11, 10, 11, 10, 10]
            assert [n() for n in v.c] == []
            assert [n() for n in v.d] == [30, 31, 32, 31, 30, 31, 32, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10], [11], [10], [11]]
            assert [[m() for m in n.c] for n in v.a] == [[], [], [], []]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30]], [[31]], [[32]], [[31]]]

    def test_entity_filter(self):
        t = self._template()
        graph = Graph(t)

        graph.append(a=-1, b= 0, d= 0)
        graph.append(a= 1, b=-1, d= 0)
        graph.append(a= 1, b= 0, d=-1)
        graph.append(a= 1, b= 1, d= 1)
        graph.append(a= 0, b= 1, d= 1)
        graph.append(a=-2, b=-2, d= 0)
        graph.append(a=-2, b= 2, d= 2)
        graph.append(a=-2, b= 1, d=-1)
        graph.append(a= 2, b= 2, d=-1)

        v = graph.view

        assert [n() for n in v.a] == [1, 0, 2]
        assert [n() for n in v.b] == [0, 1, 1, 2]
        assert [n() for n in v.d] == [1, 1]
        assert [[m() for m in n.b] for n in v.a] == [[0, 1], [1], [2]]
        assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[], [1]], [[1]], [[]]]


class TestGraphReplace:
    def _template(self):
        t = GraphTemplate([
            ("a", int, HierarchicalPolicy(lambda x:x%3), None),
            ("b", int, HierarchicalPolicy(lambda x:x%4), None),
        ])
        t.a << t.b
        return t

    def test_replace_root(self):
        graph = Graph(self._template())
        graph.append(a=1).append(a=2).append(a=3)

        assert [n() for n in graph.view.a] == [1, 2, 3]

        graph.replace(a=4).replace(a=6)

        assert [n() for n in graph.view.a] == [4, 2, 6]

    def test_child_root(self):
        graph = Graph(self._template())
        graph.append(a=1, b=0).append(a=1, b=1).append(a=1, b=2).append(a=1, b=3).append(a=2, b=3).append(a=3, b=3)

        assert [[nb() for nb in na.b] for na in graph.view.a] == [[0, 1, 2, 3], [3], [3]]

        graph.replace(a=1, b=4).replace(a=2, b=4).replace(a=1, b=7).replace(a=3, b=7)

        assert [[nb() for nb in na.b] for na in graph.view.a] == [[4, 1, 2, 7], [3, 4], [7]]


class TestGraphAdd:
    def _template(self, policy="hierarchy"):
        if policy == 'hierarchy':
            p = HierarchicalPolicy(lambda x:x)
        elif policy == 'always':
            p = AlwaysPolicy(lambda x:x)
        else:
            p = NeverPolicy(lambda x:x)

        t = GraphTemplate([
            ("a", int, p, None),
            ("b", int, p, None),
            ("c", int, p, None),
            ("d", int, p, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_add_same_graph(self, policy):
        t = self._template(policy)

        b1 = Graph(t)
        b1.append(a=0, b=10, c=20, d=30).append(a=1, b=11, c=21, d=31).append(a=2, b=12, c=22, d=32)
        b2 = Graph(t)
        b2.append(a=0, b=10, c=20, d=31).append(a=0, b=13, c=21, d=30).append(a=3, b=10, c=20, d=33)

        graph = Graph(t)
        graph += b1
        graph += b2

        v = graph.view

        if policy == "hierarchy":
            assert [n() for n in v.a] == [0, 1, 2, 3]
            assert [n() for n in v.b] == [10, 11, 12, 13, 10]
            assert [n() for n in v.c] == [20, 21, 22, 21, 20]
            assert [n() for n in v.d] == [30, 31, 32, 31, 30, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10, 13], [11], [12], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21], [21], [22], [20]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 31], [30]], [[31]], [[32]], [[33]]]
        elif policy == "always":
            assert [n() for n in v.a] == [0, 1, 2, 3]
            assert [n() for n in v.b] == [10, 11, 12, 13, 10]
            assert [n() for n in v.c] == [20, 21, 22, 21, 20]
            assert [n() for n in v.d] == [30, 31, 32, 31, 33, 30, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10, 13], [11], [12], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21], [21], [22], [20]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30, 31, 33], [30]], [[31]], [[32]], [[33]]]
        else:
            assert [n() for n in v.a] == [0, 1, 2, 0, 0, 3]
            assert [n() for n in v.b] == [10, 11, 12, 10, 13, 10]
            assert [n() for n in v.c] == [20, 21, 22, 20, 21, 20]
            assert [n() for n in v.d] == [30, 31, 32, 31, 30, 33]
            assert [[m() for m in n.b] for n in v.a] == [[10], [11], [12], [10], [13], [10]]
            assert [[m() for m in n.c] for n in v.a] == [[20], [21], [22], [20], [21], [20]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.a] == [[[30]], [[31]], [[32]], [[31]], [[30]], [[33]]]

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_add_base_graph(self, policy):
        t = self._template(policy)

        u = t + GraphTemplate([
            ("e", int, t.a.policy, None),
        ])
        u.e >> u.c

        gt = Graph(t)
        gt.append(a=0, b=10, c=20, d=30).append(a=1, b=11, c=21, d=31).append(a=2, b=12, c=22, d=32)

        gu = Graph(u)
        gu += gt
        gu.append(a=0, b=10, c=21, d=31, e=40).append(c=21, e=41)

        v = gu.view

        if policy == "hierarchy":
            assert [n() for n in v.a] == [0, 1, 2]
            assert [n() for n in v.b] == [10, 11, 12]
            assert [n() for n in v.c] == [20, 21, 22, 21]
            assert [n() for n in v.e] == [40, 41, 41]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21], [21], [22]]
            assert [[[l() for l in m.e] for m in n.c] for n in v.a] == [[[], [40, 41]], [[41]], [[]]]
        elif policy == "always":
            assert [n() for n in v.a] == [0, 1, 2]
            assert [n() for n in v.b] == [10, 11, 12]
            assert [n() for n in v.c] == [20, 21, 22, 21]
            assert [n() for n in v.e] == [40, 40, 41, 41]
            assert [[m() for m in n.c] for n in v.a] == [[20, 21], [21], [22]]
            assert [[[l() for l in m.e] for m in n.c] for n in v.a] == [[[], [40, 41]], [[40, 41]], [[]]]
        else:
            assert [n() for n in v.a] == [0, 1, 2, 0]
            assert [n() for n in v.b] == [10, 11, 12, 10]
            assert [n() for n in v.c] == [20, 21, 22, 21, 21]
            assert [n() for n in v.e] == [40, 41]
            assert [[m() for m in n.c] for n in v.a] == [[20], [21], [22], [21]]
            assert [[[l() for l in m.e] for m in n.c] for n in v.a] == [[[]], [[]], [[]], [[40]]]

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_add_moved_property(self, policy):
        t = self._template(policy)

        u = GraphTemplate([
            ("e", int, t.a.policy, None),
            ("f", t.b, None, None),
        ])
        u.f >> u.e

        gt = Graph(t)
        gt.append(a=0, b=10, c=20, d=30).append(a=0, b=11, c=21, d=31).append(a=0, b=11, c=22, d=32)

        gu = Graph(u)
        gu.append(e=40, f=10, d=31).append(e=41, f=10, d=33)
        gu += gt

        v = gu.view

        if policy == "hierarchy":
            assert [n() for n in v.e] == [40, 41]
            assert [n() for n in v.f] == [10, 10, 11]
            assert [n() for n in v.d] == [31, 33, 30, 30, 31, 32]
            assert [[m() for m in n.f] for n in v.e] == [[10], [10]]
            assert [[[l() for l in m.d] for m in n.f] for n in v.e] == [[[31, 30]], [[33, 30]]]
        elif policy == "always":
            assert [n() for n in v.e] == [40, 41]
            assert [n() for n in v.f] == [10, 10, 11]
            assert [n() for n in v.d] == [31, 33, 33, 30, 30, 31, 32]
            assert [[m() for m in n.f] for n in v.e] == [[10], [10]]
            assert [[[l() for l in m.d] for m in n.f] for n in v.e] == [[[31, 33, 30]], [[33, 30]]]
        else:
            assert [n() for n in v.e] == [40, 41]
            assert [n() for n in v.f] == [10, 10, 10, 11, 11]
            assert [n() for n in v.d] == [31, 33, 30, 31, 32]
            assert [[m() for m in n.f] for n in v.e] == [[10], [10]]
            assert [[[l() for l in m.d] for m in n.f] for n in v.e] == [[[31]], [[33]]]

    @pytest.mark.parametrize("policy", ["hierarchy", "always", "never"])
    def test_add_copied_template(self, policy):
        t = self._template(policy)

        u = GraphTemplate([
            ("e", int, t.a.policy, None),
            ("f", t, None, None),
        ])
        u.f >> u.e

        gt = Graph(t)
        gt.append(a=0, b=10, d=30).append(a=0, b=11, d=31).append(a=0, b=11, d=32)

        gu = Graph(u)
        gu.append(e=40, f=dict(a=0, b=10, d=31)).append(e=41, f=dict(a=1, b=12, d=30)).append(e=40, f=gt)

        v = gu.view

        if policy == "hierarchy":
            assert [n() for n in v.e] == [40, 41]
            assert len(v.f) == 2
            assert [n() for n in v.f[0].a] == [0]
            assert [n() for n in v.f[0].b] == [10, 11]
            assert [n() for n in v.f[0].d] == [31, 30, 31, 32]
            assert [[m() for m in n.b] for n in v.f[0].a] == [[10, 11]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.f[0].a] == [[[31, 30], [31, 32]]]
        elif policy == "always":
            assert [n() for n in v.e] == [40, 41]
            assert len(v.f) == 2
            assert [n() for n in v.f[0].a] == [0]
            assert [n() for n in v.f[0].b] == [10, 11]
            assert [n() for n in v.f[0].d] == [31, 30, 31, 32]
            assert [[m() for m in n.b] for n in v.f[0].a] == [[10, 11]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.f[0].a] == [[[31, 30], [31, 32]]]
        else:
            assert [n() for n in v.e] == [40, 41, 40]
            assert len(v.f) == 3
            assert [n() for n in v.f[0].a] == [0]
            assert [n() for n in v.f[0].b] == [10]
            assert [n() for n in v.f[0].d] == [31]
            assert [[m() for m in n.b] for n in v.f[0].a] == [[10]]
            assert [[[l() for l in m.d] for m in n.b] for n in v.f[0].a] == [[[31]]]


class TestFirstNode:
    def _template(self):
        t = GraphTemplate([
            ("a", int, None, None),
            ("b", int, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_first(self):
        g = Graph(self._template())

        g.append(a=0, b=10, c=20, d=30).append(a=1, b=11, c=21, d=31)

        assert g.view.a.b[0]() == 10
        assert g.view.a.b.d[0]() == 30

    def test_empty(self):
        g = Graph(self._template())

        assert bool(g.view.a.b) is False
        assert list(g.view.a.b) == []
        assert bool(g.view.a.b.d) is False
        assert list(g.view.a.b.d) == []
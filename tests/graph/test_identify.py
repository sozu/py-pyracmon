import pytest
from pyracmon.graph.graph import Node
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.identify import *


class TestHierarchical:
    def _prepare(self, policy):
        template = GraphTemplate([
            ("p1", int, policy, None),
            ("p2", int, policy, None),
            ("p3", int, policy, None),
        ])
        template.p1 << template.p2 << template.p3

        p1, p2, p3 = (template.p1, template.p2, template.p3)

        # 1       2       3
        # 1   2   1   2   1   2
        # 1 2 1 2 1 2 1 2 1 2 1 2
        ns1 = [Node(p1, v, policy.identifier(v), i) for i, v in enumerate([1, 2, 3])]
        ns2 = [Node(p2, v, policy.identifier(v), i) for i, v in enumerate([1, 2] * 3)]
        ns3 = [Node(p3, v, policy.identifier(v), i) for i, v in enumerate([1, 2] * 6)]

        for i in range(len(ns2)):
            ns1[int(i/2)].add_child(ns2[i])
        for i in range(len(ns3)):
            ns2[int(i/2)].add_child(ns3[i])

        return p1, p2, p3, ns1, ns2, ns3

    def _get_node(self, entity, policy, nodes):
        return [] if policy is None else [n for n in nodes if policy.identifier(n.entity) == entity]

    def test_root_new(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p1, self._get_node(4, policy, ns1), {})

        assert parents == [None]
        assert identicals == []

    def test_root_identical(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p1, self._get_node(2, policy, ns1), {})

        assert parents == []
        assert identicals == [ns1[1]]

    def test_child_root_new(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p2, self._get_node(4, policy, ns2), {})

        assert parents == [None]
        assert identicals == []

    def test_child_root_identical(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p2, self._get_node(2, policy, ns2), {})

        assert parents == []
        assert identicals == [ns2[1], ns2[3], ns2[5]]

    def test_child_new(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p2, self._get_node(4, policy, ns2), {p1.name: [ns1[1]]})

        assert parents == [ns1[1]]
        assert identicals == []

    def test_child_identical(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p2, self._get_node(2, policy, ns2), {p1.name: [ns1[1]]})

        assert parents == []
        assert identicals == [ns2[3]]

    def test_child_new_parents(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p3, self._get_node(4, policy, ns3), {p2.name: [ns2[1], ns2[3]]})

        assert parents == [ns2[1], ns2[3]]
        assert identicals == []

    def test_child_identical_parents(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p3, self._get_node(2, policy, ns3), {p2.name: [ns2[1], ns2[3]]})

        assert parents == []
        assert identicals == [ns3[3], ns3[7]]

    def test_child_identical_partial(self):
        policy = HierarchicalPolicy(lambda x:x)

        p1, p2, p3, ns1, ns2, ns3 = self._prepare(policy)

        parents, identicals = policy.identify(p3, self._get_node(2, policy, ns3), {p2.name: [ns2[1], ns2[3]]})

        assert parents == []
        assert identicals == [ns3[3], ns3[7]]
         

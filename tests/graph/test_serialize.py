import pytest
from pyracmon.graph.graph import Graph
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.serialize import *

spec = GraphSpec()

class TestSerialize:
    def _graph(self):
        t = spec.new_template(
            a = (int, lambda x:x),
            b = (int, lambda x:x),
            c = (int, lambda x:x),
            d = (),
            e = ()
        )
        t.a << t.b
        t.b << [t.c, t.e]
        t.a << t.d

        graph = Graph(t)
        graph.append(a = 1, b = 10, c = 100, d = "a", e = "A")
        graph.append(a = 1, b = 11, c = 101, d = "b", e = "B")
        graph.append(a = 2, b = 20, c = 200, d = "c", e = "C")
        graph.append(a = 2, b = 21, c = 101, d = "d", e = "D")
        graph.append(a = 2, b = 22, c = 202, d = "e", e = "E")
        graph.append(a = 3, b = 30, c = 300, d = "f", e = "F")
        graph.append(a = 3, b = 30, c = 301, d = "g", e = "G")
        graph.append(a = 3, b = 30, c = 200, d = "h", e = "H")

        return graph.view

    def test_no_serializer(self):
        assert spec.to_dict(self._graph()) == {}

    def test_default_serialize(self):
        assert spec.to_dict(
            self._graph(),
            a = (),
        ) == {"a": [1, 2, 3]}

    def test_change_name(self):
        assert spec.to_dict(
            self._graph(),
            a = ("__a__", ),
        ) == {"__a__": [1, 2, 3]}

    def test_aggregate(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, head),
        ) == {"a": 1}

    def test_aggregate_index(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, 1),
        ) == {"a": 2}

    def test_aggregate_none(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, 10),
        ) == {"a": None}

    def test_serialize(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, None, lambda s, x:x*2),
        ) == {"a": [2, 4, 6]}

    def test_ignore_child(self):
        assert spec.to_dict(
            self._graph(),
            a = (),
            b = (),
        ) == {"a": [1, 2, 3]}

    def test_include_child(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, None, lambda s, x: {"value": x}),
            b = (),
        ) == {"a": [{"value": 1, "b": [10, 11]}, {"value": 2, "b": [20, 21, 22]}, {"value": 3, "b": [30]}]}

    def test_merge_child(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, None, lambda s, x: {"value": x}),
            b = (lambda x: f"b_{x}", head, lambda s, x: {"value": x, "value2": x*2}),
        ) == {"a": [{"value": 1, "b_value": 10, "b_value2": 20}, {"value": 2, "b_value": 20, "b_value2": 40}, {"value": 3, "b_value": 30, "b_value2": 60}]}

    def test_multi_parents(self):
        assert spec.to_dict(
            self._graph(),
            a = (None, None, lambda s, x: {"va": x}),
            b = (None, None, lambda s, x: {"vb": x}),
            c = (),
            d = (None, lambda x: ''.join(x)),
            e = ("__e__",),
        ) == {
            "a": [
                {"va": 1, "d": "ab", "b": [
                    {"vb": 10, "c": [100], "__e__":["A"]},
                    {"vb": 11, "c": [101], "__e__":["B"]},
                ]},
                {"va": 2, "d": "cde", "b": [
                    {"vb": 20, "c": [200], "__e__":["C"]},
                    {"vb": 21, "c": [101], "__e__":["D"]},
                    {"vb": 22, "c": [202], "__e__":["E"]},
                ]},
                {"va": 3, "d": "fgh", "b": [
                    {"vb": 30, "c": [300, 301, 200], "__e__":["F","G","H"]}
                ]},
            ],
        }


class TestNodeSerializer:
    def _graph(self):
        t = spec.new_template(
            a = int,
        )

        graph = Graph(t)
        graph.append(a = 1)
        graph.append(a = 2)
        graph.append(a = 3)

        return graph.view

    def test_default(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of(),
        ) == {"a": [1,2,3]}

    def test_spec_serializer(self):
        spec = GraphSpec()
        spec.add_serializer(int, lambda s, x: x*2)
        assert spec.to_dict(
            self._graph(),
            a = S.of(),
        ) == {"a": [2,4,6]}

    def test_name(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().name("A"),
        ) == {"A": [1,2,3]}

    def test_merge(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().merge(lambda n: f"__{n}__"),
        ) == {}

    def test_head(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().head(),
        ) == {"a": 1}

    def test_tail(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().tail(),
        ) == {"a": 3}

    def test_fold(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().fold(sum),
        ) == {"a": 6}

    def test_each(self):
        assert spec.to_dict(
            self._graph(),
            a = S.of().each(lambda s,x: x*3),
        ) == {"a": [3,6,9]}

    def test_propagate(self):
        def old(s, v):
            return v*2
        assert spec.to_dict(
            self._graph(),
            a = S.of().each(old).each(lambda s,x: s(x)*3),
        ) == {"a": [6,12,18]}
        

class TestFactory:
    def _graph(self):
        t = spec.new_template(
            a = int,
        )

        graph = Graph(t)
        graph.append(a = 1)
        graph.append(a = 2)
        graph.append(a = 3)

        return graph.view

    def test_name(self):
        assert spec.to_dict(
            self._graph(),
            a = S.name("A")
        ) == {"A": [1,2,3]}

    def test_merge(self):
        assert spec.to_dict(
            self._graph(),
            b = S.merge(lambda n: f"__{n}__")
        ) == {}

    def test_at(self):
        assert spec.to_dict(
            self._graph(),
            a = S.at(1)
        ) == {"a": 2}

    def test_head(self):
        assert spec.to_dict(
            self._graph(),
            a = S.head()
        ) == {"a": 1}

    def test_tail(self):
        assert spec.to_dict(
            self._graph(),
            a = S.tail()
        ) == {"a": 3}

    def test_fold(self):
        assert spec.to_dict(
            self._graph(),
            a = S.fold(sum)
        ) == {"a": 6}

    def test_each(self):
        assert spec.to_dict(
            self._graph(),
            a = S.each(lambda s,x: x*2)
        ) == {"a": [2,4,6]}
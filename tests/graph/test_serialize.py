import pytest
from pyracmon.graph.template import graph_template
from pyracmon.graph.graph import Graph
from pyracmon.graph.serialize import *


class TestSerialize:
    def _graph(self):
        t = graph_template(
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
        assert graph_dict(self._graph()) == {}

    def test_default_serialize(self):
        assert graph_dict(
            self._graph(),
            a = (),
        ) == {"a": [1, 2, 3]}

    def test_change_name(self):
        assert graph_dict(
            self._graph(),
            a = (lambda n: f"__{n}__", ),
        ) == {"__a__": [1, 2, 3]}

    def test_aggregate(self):
        assert graph_dict(
            self._graph(),
            a = (None, head),
        ) == {"a": 1}

    def test_serialize(self):
        assert graph_dict(
            self._graph(),
            a = (None, None, lambda x:x*2),
        ) == {"a": [2, 4, 6]}

    def test_ignore_child(self):
        assert graph_dict(
            self._graph(),
            a = (),
            b = (),
        ) == {"a": [1, 2, 3]}

    def test_include_child(self):
        assert graph_dict(
            self._graph(),
            a = (None, None, lambda x: {"value": x}),
            b = (),
        ) == {"a": [{"value": 1, "b": [10, 11]}, {"value": 2, "b": [20, 21, 22]}, {"value": 3, "b": [30]}]}

    def test_multi_parents(self):
        assert graph_dict(
            self._graph(),
            a = (None, None, lambda x: {"va": x}),
            b = (None, None, lambda x: {"vb": x}),
            c = (),
            d = (None, lambda x: ''.join(x)),
            e = (lambda n: f"__{n}__",),
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

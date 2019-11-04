import pytest
from pyracmon.graph.template import graph_template
from pyracmon.graph.graph import *


class TestView:
    def _template(self):
        t = graph_template(
            a = (),
            b = (),
            c = (),
            d = (),
            e = (),
        )
        t.a << [t.b, t.c]
        t.b << t.d
        return t

    def test_node_view(self):
        t = self._template()
        n = Node(t.a, 1)
        n.add_child(Node(t.b, 2))
        n.add_child(Node(t.c, 3))
        n.add_child(Node(t.c, 4))
        view = n.view

        assert view() == 1
        assert [n() for n in view.b] == [2]
        assert [n() for n in view.c] == [3, 4]
        assert [[n() for n in c] for k, c in view] == [[2], [3, 4]]

    def test_container_view(self):
        t = self._template()
        c = NodeContainer(t.a)
        c.append(1)
        c.append(2)
        c.append(3)
        view = c.view

        assert view() == c
        assert len(view) == 3
        assert [n() for n in view] == [1, 2, 3]
        assert [view[0](), view[1](), view[2]()] == [1, 2, 3]

    def test_graph_view(self):
        view = Graph(self._template()).view

        assert [c().name for c in view] == ["a", "e"]
        assert view.a().name == "a"
        assert view.b().name == "b"
        assert view.c().name == "c"
        assert view.d().name == "d"
        assert view.e().name == "e"


class TestGraph:
    def test_append(self):
        graph = Graph(graph_template(
            a = (),
            b = (),
        ))
        graph.append(a = 1, b = "a")
        graph.append(a = 2, b = "b")
        graph.append(a = 3, b = "c")

        assert [n() for n in graph.view.a] == [1, 2, 3]
        assert [n() for n in graph.view.b] == ["a", "b", "c"]

    def test_relation(self):
        t = graph_template(
            a = (),
            b = (),
        )
        t.b >> t.a
        graph = Graph(t)

        graph.append(a = 1, b = "a")
        graph.append(a = 2, b = "b")
        graph.append(a = 3, b = "c")

        assert [n() for n in graph.view.a[0].b] == ["a"]
        assert [n() for n in graph.view.a[1].b] == ["b"]
        assert [n() for n in graph.view.a[2].b] == ["c"]

    def test_identify(self):
        t = graph_template(
            a = (None, lambda x:x%3),
            b = (),
        )
        t.b >> t.a
        graph = Graph(t)

        graph.append(a = 1, b = "a")
        graph.append(a = 4, b = "b")
        graph.append(a = 7, b = "c")
        graph.append(a = 2, b = "d")
        graph.append(a = 5, b = "e")
        graph.append(a = 3, b = "f")

        assert len(graph.view.a) == 3
        assert [n() for n in graph.view.a] == [1, 2, 3]
        assert [n() for n in graph.view.a[0].b] == ["a", "b", "c"]
        assert [n() for n in graph.view.a[1].b] == ["d", "e"]
        assert [n() for n in graph.view.a[2].b] == ["f"]

    def test_multi_parents(self):
        t = graph_template(
            a = (None, lambda x:x),
            b = (None, lambda x:x),
            c = (None, lambda x:x),
        )
        t.a << t.b << t.c
        graph = Graph(t)

        graph.append(a = 1, b = 10, c = 100)
        graph.append(a = 1, b = 11, c = 101)
        graph.append(a = 2, b = 20, c = 200)
        graph.append(a = 2, b = 21, c = 101)
        graph.append(a = 2, b = 21, c = 201)
        graph.append(a = 3, b = 30, c = 101)
        view = graph.view

        assert len(graph.view.a) == 3
        assert [n() for n in graph.view.a] == [1, 2, 3]
        assert [n() for n in graph.view.b] == [10, 11, 20, 21, 30]
        assert [n() for n in graph.view.c] == [100, 101, 200, 201]
        assert [n() for n in graph.view.a[0].b] == [10, 11]
        assert [n() for n in graph.view.a[0].b[0].c] == [100]
        assert [n() for n in graph.view.a[0].b[1].c] == [101]
        assert [n() for n in graph.view.a[1].b] == [20, 21]
        assert [n() for n in graph.view.a[1].b[0].c] == [200]
        assert [n() for n in graph.view.a[1].b[1].c] == [101, 201]
        assert [n() for n in graph.view.a[2].b] == [30]
        assert [n() for n in graph.view.a[2].b[0].c] == [101]


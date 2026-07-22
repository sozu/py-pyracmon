import pytest
from dataclasses import dataclass
from pyracmon.graph.graph import nest
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.typed import *


@dataclass
class ent1:
    v1: int
@dataclass
class ent2:
    v2: str
@dataclass
class ent3:
    v3: float


def test_node_declaration():
    class n5(TNode[ent3]):
        a5: float
    class n4(TNode[ent3]):
        a4: float
    class n3(TNode[ent3]):
        a3: float
    class n2(TNode[ent2]):
        a2: str
    class n1(TNode[ent1]):
        a1: int
        b1: int | None
        c1: TEdge[int]
        d1: TEdge[int | None]
        e1: n2
        f1: n3 | None
        g1: TEdge[n4]
        h1: TEdge[n5 | None]

    assert n1._entity_type_ is ent1
    assert n2._entity_type_ is ent2
    assert n3._entity_type_ is ent3

    assert n1._fields_ == {
        "a1": valuer(n1, "a1", int, False),
        "b1": valuer(n1, "b1", int, True),
        "c1": edger(n1, "c1", int, False),
        "d1": edger(n1, "d1", int, False),
        "e1": noder(n1, "e1", n2, False),
        "f1": noder(n1, "f1", n3, True),
        "g1": edger(n1, "g1", n4, True),
        "h1": edger(n1, "h1", n5, True),
    }
    assert n2._fields_ == {"a2": valuer(n2, "a2", str, False)}
    assert n3._fields_ == {"a3": valuer(n3, "a3", float, False)}


def test_graph_declaration():
    class n4(TNode[ent1]):
        a4: int
    class n3(TNode[ent3]):
        a3: float
    class n2(TNode[ent2]):
        a21: n3
        a22: str
    class n1(TNode[ent1]):
        a1: int
    class g(TGraph):
        g1: n1
        g2: TEdge[n2]
        g3: int
        g4: TEdge[n4 | None]

    assert g._fields_ == {
        "g1": noder(g, "g1", n1, False),
        "g2": edger(g, "g2", n2, True),
        "g3": valuer(g, "g3", int, False),
        "g4": edger(g, "g4", n4, True),
    }
    assert g._properties_ == {
        "g1": ent1, "g2": ent2, "g3": int, "g4": ent1,
        "a1": int,
        "a21": ent3, "a22": str,
        "a3": float,
        "a4": int,
    }
    assert g._relations_ == [("g1", "a1"), ("g2", "a21"), ("g2", "a22"), ("a21", "a3"), ("g4", "a4")]


def test_flat_value():
    class g(TGraph):
        a: int
        b: int | None
        c: TEdge[int]
        d: TEdge[int | None]

    v = new_typed_graph(g).append(
        a=10, b=None, c=20, d=None
    ).append(
        a=11, b=12, c=21, d=22
    ).append(
        a=12, b=13, c=22, d=None
    ).view

    assert v.a == 10
    assert v.b is None
    assert list(v.c) == [20, 21, 22]
    assert list(v.d) == [None, 22, None]


def test_flat_node():
    class n1(TNode[ent1]):
        a1: int
    class n2(TNode[ent1]):
        a2: int
    class n3(TNode[ent2]):
        a3: int
    class n4(TNode[ent3]):
        a4: int
    class g(TGraph):
        a: n1
        b: n2 | None
        c: TEdge[n3]
        d: TEdge[n4 | None]

    v = new_typed_graph(g).append(
        a=ent1(10), b=None, c=ent2("a"), d=None,
        a1=11, a2=21, a3=31, a4=41,
    ).append(
        a=ent1(11), b=ent1(12), c=ent2("b"), d=ent3(22.0),
        a1=12, a2=22, a3=32, a4=42,
    ).append(
        a=ent1(12), b=None, c=ent2("c"), d=None,
        a1=13, a2=23, a3=33, a4=43,
    ).view

    assert v.a() == ent1(10)
    assert v.a.a1 == 11
    assert v.b is None
    assert [n() for n in v.c] == [ent2("a"), ent2("b"), ent2("c")]
    c2 = v.c.find(lambda n: n.a3 == 32)
    assert c2 and c2() is v.c[1]()
    assert [n.a3 for n in v.c] == [31, 32, 33]
    assert [(n and n()) for n in v.d] == [None, ent3(22.0), None]
    assert [(n and n.a4) for n in v.d] == [None, 42, None]


def test_nested():
    class n1(TNode[ent1]):
        c1: int
    class n2(TNode[ent1]):
        c2: n1
    class n3(TNode[ent1]):
        c3: n2
    class g(TGraph):
        a: n3

    v = new_typed_graph(g).append(
        a=ent1(10), c3=ent1(20), c2=ent1(30), c1=40,
    ).append(
        a=ent1(11), c3=ent1(21), c2=ent1(31), c1=41,
    ).append(
        a=ent1(12), c3=ent1(22), c2=ent1(32), c1=42,
    ).view

    assert v.a() == ent1(10)
    assert v.a.c3() == ent1(20)
    assert v.a.c3.c2() == ent1(30)
    assert v.a.c3.c2.c1 == 40


@pytest.mark.parametrize("level", [0, 1, 2])
def test_nested_opt(level: int):
    class n1(TNode[ent1]):
        c1: int
    class n2(TNode[ent1]):
        c2: n1 | None
    class n3(TNode[ent1]):
        c3: n2 | None
    class g(TGraph):
        a: n3 | None

    match level:
        case 0:
            v = new_typed_graph(g).append(
                a=None
            ).append(
                a=ent1(11), c3=ent1(21), c2=ent1(31), c1=41,
            ).view
            assert v.a is None
        case 1:
            v = new_typed_graph(g).append(
                a=ent1(10), c3=None,
            ).append(
                a=ent1(11), c3=ent1(21), c2=ent1(31), c1=41,
            ).view
            assert v.a
            assert v.a() == ent1(10)
            assert v.a.c3 is None
        case 2:
            v = new_typed_graph(g).append(
                a=ent1(10), c3=ent1(20), c2=None,
            ).append(
                a=ent1(11), c3=ent1(21), c2=ent1(31), c1=41,
            ).view
            assert v.a
            assert v.a() == ent1(10)
            assert v.a.c3
            assert v.a.c3() == ent1(20)
            assert v.a.c3.c2 is None


def test_nested_edge():
    class n1(TNode[ent1]):
        c1: int
    class n2(TNode[ent1]):
        c2: TEdge[n1]
    class n3(TNode[ent1]):
        c3: TEdge[n2]
    class g(TGraph):
        a: TEdge[n3]

    v = new_typed_graph(g).append(
        a=ent1(10), c3=ent1(20), c2=ent1(30), c1=40,
    ).append(
        a=ent1(11), c3=ent1(21), c2=ent1(31), c1=41,
    ).append(
        a=ent1(12), c3=ent1(22), c2=ent1(32), c1=42,
    ).view

    assert [n() for n in v.a] == [ent1(10), ent1(11), ent1(12)]
    assert [n() for na in v.a for n in na.c3] == [ent1(20), ent1(21), ent1(22)]
    assert [n() for na in v.a for n3 in na.c3 for n in n3.c2] == [ent1(30), ent1(31), ent1(32)]
    assert [n.c1 for na in v.a for n3 in na.c3 for n in n3.c2] == [40, 41, 42]


def test_nested_edge_opt():
    class n1(TNode[ent1]):
        c1: int
    class n2(TNode[ent1]):
        c2: TEdge[n1 | None]
    class n3(TNode[ent1]):
        c3: TEdge[n2 | None]
    class g(TGraph):
        a: TEdge[n3 | None]

    v = new_typed_graph(g).append(
        a=ent1(10), c3=ent1(20), c2=None, c1=40,
    ).append(
        a=ent1(11), c3=None, c2=ent1(31), c1=41,
    ).append(
        a=None, c3=ent1(22), c2=ent1(32), c1=42,
    ).view

    assert dump_typed_graph(v) == {
        "a": [
            (ent1(10), {
                "c3": [
                    (ent1(20), {
                        "c2": [
                            None,
                        ],
                    }),
                ],
            }),
            (ent1(11), {
                "c3": [
                    None,
                ],
            }),
            None,
        ]
    }


def test_ident():
    class n1(TNode[ent3]):
        c1: int | None
    class n2(TNode[ent2]):
        c2: TEdge[n1 | None]
    class n3(TNode[ent1]):
        c3: n2 | None
    class g(TGraph):
        a: TEdge[n3]

    spec = GraphSpec().add_identifier(ent1, lambda e: e.v1).add_identifier(ent2, lambda e: e.v2)

    v = new_typed_graph(g, spec).append(
        a=ent1(10), c3=ent2("a"), c2=ent3(30.0), c1=40,
    ).append(
        a=ent1(11), c3=None, c2=ent3(31.1), c1=41,
    ).append(
        a=ent1(10), c3=ent2("b"), c2=ent3(32.2), c1=42,
    ).append(
        a=ent1(10), c3=ent2("a"), c2=None, c1=44,
    ).append(
        a=ent1(10), c3=ent2("a"), c2=ent3(33.3), c1=43,
    ).append(
        a=ent1(12), c3=ent2("b"), c2=ent3(34.4),
    ).view

    assert dump_typed_graph(v) == {
        "a": [
            (ent1(10), {
                "c3": (ent2("a"), {
                    "c2": [
                        (ent3(30.0), {"c1": 40}),
                        None,
                        (ent3(33.3), {"c1": 43}),
                    ],
                }),
            }),
            (ent1(11), {
                "c3": None,
            }),
            (ent1(12), {
                "c3": (ent2("b"), {
                    "c2": [
                        (ent3(34.4), {"c1": None}),
                    ],
                }),
            }),
        ],
    }


def test_mixed():
    # g -+- e[n1|] --+-- e[n2] ----- s|
    #    |           |
    #    |           +-- n3|   --+-- e[s]
    #    |           |           +-- i
    #    |           +-- i
    #    +- e[s]
    #    +- i
    class n3(TNode[ent3]):
        c31: TEdge[str]
        c32: int
    class n2(TNode[ent2]):
        c21: str | None
    class n1(TNode[ent1]):
        c11: TEdge[n2]
        c12: n3 | None
        c13: int
    class g(TGraph):
        a1: TEdge[n1 | None]
        a2: TEdge[str]
        a3: int

    spec = GraphSpec().add_identifier(ent1, lambda e: e.v1).add_identifier(ent2, lambda e: e.v2)

    v = new_typed_graph(g, spec).append(
        a1=ent1(10), a2="a", a3=100, c11=ent2("x"), c12=ent3(30.0), c13=40, c21=None, c31="o", c32=300,
    ).append(
        a1=ent1(10), a2="b", c11=ent2("x"), c12=None, c13=41, c21="h", c31="", c32=0,
    ).append(
        a1=ent1(10), a2="c", c11=ent2("y"), c12=None, c13=42, c21="i",
    ).append(
        a1=ent1(10), a2="c", c11=ent2("y"), c12=ent3(33.3), c13=43, c21="j",
    ).append(
        a1=ent1(11), a2="d", a3=101, c11=ent2("x"), c12=None, c13=44, c21="k", c31="p", c32=400,
    ).append(
        a1=None, c11=ent2("z"), c12=None, c13=0, c31="q", c32=0,
    ).append(
        a1=ent1(11), a2="e", a3=102, c11=ent2("y"), c12=ent3(35.5), c13=45, c21="l", c31="r", c32=500,
    ).append(
        a1=ent1(12), a3=103, c13=46,
    ).append(
        a1=ent1(13), a2="f", a3=103, c11=ent2("z"), c12=ent3(36.6), c13=47, c21=None, c32=600,
    ).view

    assert dump_typed_graph(v) == {
        "a1": [
            (ent1(10), {
                "c11": [
                    (ent2("x"), {"c21": None}),
                    (ent2("y"), {"c21": "i"}),
                ],
                "c12": (ent3(30.0), {
                    "c31": ["o"],
                    "c32": 300,
                }),
                "c13": 40,
            }),
            (ent1(11), {
                "c11": [
                    (ent2("x"), {"c21": "k"}),
                    (ent2("y"), {"c21": "l"}),
                ],
                "c12": None,
                "c13": 44,
            }),
            None,
            (ent1(12), {
                "c11": [],
                "c12": None,
                "c13": 46,
            }),
            (ent1(13), {
                "c11": [
                    (ent2("z"), {"c21": None}),
                ],
                "c12": (ent3(36.6), {
                    "c31": [],
                    "c32": 600,
                }),
                "c13": 47,
            }),
        ],
        "a2": ["a", "b", "c", "c", "d", "e", "f"],
        "a3": 100,
    }


def test_methods_available():
    class n1(TNode[ent1]):
        c11: int

        @property
        def prop(self) -> str:
            return f"prop-{self.c11}"

        @classmethod
        def class_method(cls: type, v: int) -> str:
            return f"class_method-{v}"

        def method(self, v: int) -> str:
            return f"method-{v+self.c11}"

    class g(TGraph):
        a1: n1

    assert n1._fields_ == {
        "c11": valuer(n1, "c11", int, False),
    }
    assert n1.class_method(10) == "class_method-10"

    v = new_typed_graph(g).append(a1=ent1(1), c11=10).view
    assert v.a1.c11 == 10
    assert v.a1.prop == "prop-10"
    assert v.a1.method(5) == "method-15"


def test_nested_graph():
    class n4(TNode[ent1]):
        a: int
    class n3(TNode[ent3]):
        a: float
    class n2(TNode[ent2]):
        a3: TEdge[n3]
        a4: TEdge[n4]
    class n1(TNode[ent1]):
        a: TEdge[int]
    class g(TGraph, nested=True):
        g1: n1
        g2: TEdge[n2]

    assert g._fields_ == {
        "g1": noder(g, "g1", n1, False),
        "g2": edger(g, "g2", n2, True),
    }
    assert g._properties_ == {
        "g1": ent1, "g2": ent2,
        "g1.a": int,
        "g2.a3": ent3, "g2.a4": ent1,
        "g2.a3.a": float,
        "g2.a4.a": int,
    }
    assert g._relations_ == [("g1", "g1.a"), ("g2", "g2.a3"), ("g2", "g2.a4"), ("g2.a3", "g2.a3.a"), ("g2.a4", "g2.a4.a")]

    spec = GraphSpec().add_identifier(ent1, lambda e: e.v1).add_identifier(ent2, lambda e: e.v2)

    v = new_typed_graph(g, spec).append(
        g1=nest(ent1(10), a=1),
        g2=nest(ent2("2"), a3=nest(ent3(3.0), a=4.0), a4=nest(ent1(5), a=6)),
    ).append(
        g1=nest(ent1(10), a=2),
        g2=nest(ent2("2"), a3=nest(ent3(3.1), a=4.1), a4=nest(ent1(10), a=12)),
    ).append(
        g1=nest(ent1(10), a=3),
        g2=nest(ent2("3"), a3=nest(ent3(3.2), a=4.2), a4=nest(ent1(15), a=18)),
    ).append(
        g1=nest(ent1(11), a=3),
        g2=nest(ent2("3"), a3=nest(ent3(3.3), a=4.3), a4=nest(ent1(15), a=21)),
    ).append(
        g1=nest(ent1(12), a=3),
        g2=nest(ent2("4"), a3=nest(ent3(3.2), a=4.4), a4=nest(ent1(15), a=24)),
    ).view

    assert v.g1() == ent1(10)
    assert list(v.g1.a) == [1, 2, 3]
    assert [n() for n in v.g2] == [ent2("2"), ent2("3"), ent2("4")]

    assert [n() for n in v.g2[0].a3] == [ent3(3.0), ent3(3.1)]
    assert [n.a for n in v.g2[0].a3] == [4.0, 4.1]
    assert [n() for n in v.g2[0].a4] == [ent1(5), ent1(10)]
    assert [n.a for n in v.g2[0].a4] == [6, 12]

    assert [n() for n in v.g2[1].a3] == [ent3(3.2), ent3(3.3)]
    assert [n.a for n in v.g2[1].a3] == [4.2, 4.3]
    assert [n() for n in v.g2[1].a4] == [ent1(15)]
    assert [n.a for n in v.g2[1].a4] == [18]

    assert [n() for n in v.g2[2].a3] == [ent3(3.2)]
    assert [n.a for n in v.g2[2].a3] == [4.4]
    assert [n() for n in v.g2[2].a4] == [ent1(15)]
    assert [n.a for n in v.g2[2].a4] == [24]
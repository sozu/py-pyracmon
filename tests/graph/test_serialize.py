import pytest
from typing import Generic, TypeVar
from inspect import signature, Signature
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.graph import Graph, Node
from pyracmon.graph.identify import HierarchicalPolicy
from pyracmon.graph.serialize import *
from pyracmon.graph.schema import Typeable, issubgeneric


class TestS:
    def test_no_arg(self):
        ns = S.of()
        assert ns._namer is None
        assert ns._aggregator is None
        assert ns._serializers == []

    def test_args(self):
        namer = lambda n: n
        agg = lambda vs: vs
        ser1, ser2, ser3 = [(lambda x:x) for i in range(3)]

        ns = S.of(namer, agg, ser1, ser2, ser3)

        assert ns._namer is namer
        assert ns._aggregator is agg
        assert ns._serializers == [ser1, ser2, ser3]


class TestNamer:
    def test_no_namer(self):
        ns = NodeSerializer()

        assert ns.namer("a") == "a"
        assert not ns.be_merged

    def test_str_namer(self):
        ns = NodeSerializer()
        ns.name("abc")

        assert ns.namer("a") == "abc"
        assert not ns.be_merged

    def test_callable_namer(self):
        ns = NodeSerializer()
        ns.merge(lambda n: f"__{n}__")

        assert ns.namer("a") == "__a__"
        assert ns.be_merged


class TestAggregator:
    def test_no_aggregator(self):
        ns = NodeSerializer()
        a = ns.aggregator
        r = a([1, 2, 3])

        assert not ns.be_singular
        assert r == [1, 2, 3]

    def test_nosig_fold(self):
        ns = NodeSerializer()
        def agg(vs):
            return vs[2]
        ns.fold(agg)
        a = ns.aggregator
        r = a([1, 2, 3])

        assert ns.be_singular
        assert r == 3

    def test_nosig_select(self):
        ns = NodeSerializer()
        def agg(vs):
            return vs[0:2]
        ns.select(agg)
        a = ns.aggregator
        r = a([1, 2, 3])

        assert not ns.be_singular
        assert r == [1, 2]

    def test_fold(self):
        ns = NodeSerializer()
        def agg(vs: [int]) -> int:
            return vs[2]
        ns.fold(agg)
        a = ns.aggregator
        r = a([1, 2, 3])

        assert ns.be_singular
        assert signature(a).return_annotation is int
        assert r == 3

    def test_select(self):
        ns = NodeSerializer()
        def agg(vs: [int]) -> [int]:
            return vs[0:2]
        ns.select(agg)
        a = ns.aggregator
        r = a([1, 2, 3])

        assert not ns.be_singular
        assert signature(a).return_annotation == [int]
        assert r == [1, 2]

    def test_invalid_fold(self):
        ns = NodeSerializer()
        def agg(vs: [int]) -> [int]:
            return vs[0:2]

        with pytest.raises(ValueError):
            ns.fold(agg)

    def test_invalid_select(self):
        ns = NodeSerializer()
        def agg(vs: [int]) -> int:
            return vs[0:2]

        with pytest.raises(ValueError):
            ns.select(agg)

    def test_at(self):
        ns = NodeSerializer()
        ns.at(1, 100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([1, 2, 3]) == 2
        assert a([1]) == 100

    def test_head(self):
        ns = NodeSerializer()
        ns.head(100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([1, 2, 3]) == 1
        assert a([]) == 100

    def test_last(self):
        ns = NodeSerializer()
        ns.last(100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([1, 2, 3]) == 3
        assert a([]) == 100


class TestSerializer:
    def _template(self):
        return GraphTemplate([
            ("a", int, None, None),
        ])

    def test_no_serializer(self):
        t = self._template()
        ns = NodeSerializer()
        s = ns.serializer
        r = s(None, Node(t.a, 5, 0, 0).view, None, 5)

        assert signature(s).return_annotation is Signature.empty
        assert r == 5

    def test_serializer(self):
        t = self._template()
        ns = NodeSerializer()
        ct = 0
        def f1(v) -> int:
            nonlocal ct
            ct += 1
            return v+1
        def f2(b, v) -> float:
            nonlocal ct
            ct += 1
            vv = b(v)
            return vv*1.3
        def f3(n, b, v) -> str:
            nonlocal ct
            ct += 1
            vv = b(v)
            return f"{n.entity + vv}"
        s = ns.each(f1).each(f2).each(f3).serializer
        r = s(None, Node(t.a, 5, 0, 0), None, 5)

        assert signature(s).return_annotation is str
        assert ct == 3
        assert r == "12.8"

    def test_partial_annotation(self):
        t = self._template()
        ns = NodeSerializer()
        def f1(v):
            return v+1
        def f2(b, v) -> float:
            vv = b(v)
            return vv*1.3
        def f3(n, b, v):
            vv = b(v)
            return f"{n.entity + vv}"
        s = ns.each(f1).each(f2).each(f3).serializer
        r = s(None, Node(t.a, 5, 0, 0), None, 5)

        assert signature(s).return_annotation is float
        assert r == "12.8"

    def test_generic(self):
        T = TypeVar("T")
        class G(Generic[T]):
            pass
        t = self._template()
        ns = NodeSerializer()
        def f0(v):
            return v
        def f1(v) -> int:
            return v
        def f2(b, v) -> G[T]:
            return v
        def f3(n, b, v) -> G[T]:
            return v
        s = ns.each(f0).each(f1).each(f2).each(f3).serializer

        assert signature(s).return_annotation == G[G[int]]


class TestSubGraph:
    def _template(self):
        t = GraphTemplate([
            ("a", dict, None, None),
            ("b", dict, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def test_sub(self):
        graph = Graph(self._template())

        graph.append(a=dict(a0=0, a1=1), b=dict(b0=10, b1=11), c=20, d=30)
        graph.append(a=dict(a0=2, a1=3), b=dict(b0=12, b1=13), c=21, d=31)

        t = GraphTemplate([
            ("a", int, None, None),
            ("t", graph.template),
        ])
        ns = NodeSerializer()
        s = ns.sub(a=S.of(), b=S.of(), c=S.of(), d=S.of()).serializer
        r = s(
            SerializationContext({}, lambda t:[]),
            Node(t.t, graph, None, 0),
            None,
            graph,
        )

        assert ns.be_singular
        assert issubgeneric(signature(s).return_annotation, Typeable)
        assert r == {
            "a": [
                {
                    "a0": 0, "a1": 1,
                    "b": [
                        {
                            "b0": 10, "b1": 11,
                            "d": [30],
                        },
                    ],
                    "c": [20],
                },
                {
                    "a0": 2, "a1": 3,
                    "b": [
                        {
                            "b0": 12, "b1": 13,
                            "d": [31],
                        },
                    ],
                    "c": [21],
                },
            ]
        }


class TestContext:
    def _template(self):
        t = GraphTemplate([
            ("a", int, HierarchicalPolicy(lambda x:x), None),
            ("b", int, HierarchicalPolicy(lambda x:x), None),
            ("c", int, HierarchicalPolicy(lambda x:x), None),
            ("d", int, HierarchicalPolicy(lambda x:x), None),
        ])
        t.a << [t.d >> t.b, t.c]
        return t

    def _graph(self):
        graph = Graph(self._template())

        graph.append(a=0, b=10, c=20, d=30)
        graph.append(a=0, b=10, c=21, d=31)
        graph.append(a=1, b=11, c=20, d=30)
        graph.append(a=1, b=12, c=20, d=30)
        graph.append(a=2, b=10, c=20, d=30)
        graph.append(a=2, b=11, c=21, d=30)

        return graph.view

    def test_no_serializer(self):
        ser_map = dict()
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {}

    def test_default(self):
        ser_map = dict(a = S.of(), b = S.of(), c = S.of(), d = S.of())
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [0, 1, 2]}

    def test_finder(self):
        ser_map = dict(a = S.of(), b = S.of(), c = S.of(), d = S.of())
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[lambda v:v*2])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [0, 2, 4]}

    def test_full_arguments_serializer(self):
        ser_map = dict(a = S.of(), b = S.of(), c = S.of(), d = S.of())
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[lambda c,n,b,v:v*2])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [0, 2, 4]}

    def test_put_child(self):
        ser_map = dict(
            a = S.each(lambda v: {"A": v}),
            b = S.each(lambda v: {"B": v}),
            d = S.of(),
        )
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0, "b": [
                {"B": 10, "d": [30, 31]},
            ]},
            {"A": 1, "b": [
                {"B": 11, "d": [30]}, {"B": 12, "d": [30]},
            ]},
            {"A": 2, "b": [
                {"B": 10, "d": [30]}, {"B": 11, "d": [30]},
            ]},
        ]}

    def test_ignore_child(self):
        ser_map = dict(
            a = S.each(lambda v: {"A": v}),
            d = S.of(),
        )
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0}, {"A": 1}, {"A": 2},
        ]}

    def test_name(self):
        ser_map = dict(a = S.name("A"))
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"A": [0, 1, 2]}

    def test_fold(self):
        ser_map = dict(a = S.head())
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": 0}

    def test_merge(self):
        ser_map = dict(
            a = S.each(lambda v: {"A": v}),
            b = S.head().each(lambda v: {"B": v}).merge(lambda n:f"__{n}__"),
        )
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0, "__B__": 10},
            {"A": 1, "__B__": 11},
            {"A": 2, "__B__": 10},
        ]}

    def test_merge_root(self):
        ser_map = dict(
            a = S.merge().each(lambda v: {"a1": v, "a2": v+1}),
            b = S.head().each(lambda v: {"B": v}).merge(lambda n:f"__{n}__"),
        )
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {
            "a1": 0, "a2": 1, "__B__": 10,
        }

    def test_alter_extend(self):
        ser_map = dict(a = S.each(lambda v: {"A": v, "B": v+1, "C": v+2}).alter(lambda x: {"D": x*3}))
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0, "B": 1, "C": 2, "D": 0},
            {"A": 1, "B": 2, "C": 3, "D": 3},
            {"A": 2, "B": 3, "C": 4, "D": 6},
        ]}

    def test_alter_shrink(self):
        ser_map = dict(a = S.each(lambda v: {"A": v, "B": v+1, "C": v+2}).alter(excludes=["B"]))
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0, "C": 2},
            {"A": 1, "C": 3},
            {"A": 2, "C": 4},
        ]}

    def test_alter_includes(self):
        ser_map = dict(a = S.each(lambda v: {"A": v, "B": v+1, "C": v+2}).alter(excludes=["B"], includes={"A"}))
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {"A": 0},
            {"A": 1},
            {"A": 2},
        ]}

    def test_all(self):
        def f1(v):
            return v+1
        def f2(b, v) -> float:
            vv = b(v)
            return vv*2.0
        def f3(n, b, v):
            vv = b(v)
            return f"{n.entity + vv}"

        ser_map = dict(
            a = S.each(lambda v: {"A": v}),
            b = S.name("x").each(lambda v: {"B": v}),
            c = S.each(f1).each(f2).each(f3),
            d = S.last().merge(lambda n:f"__{n}__").each(lambda v: {"D": v}),
        )
        r = {}
        cxt = SerializationContext(ser_map, lambda x:[])
        cxt.serialize_to("a", self._graph().a, r)

        assert r == {"a": [
            {
                "A": 0,
                "x": [
                    {"B": 10, "__D__": 31},
                ],
                "c": ["62.0", "65.0"],
            },
            {
                "A": 1,
                "x": [
                    {"B": 11, "__D__": 30},
                    {"B": 12, "__D__": 30},
                ],
                "c": ["62.0"],
            },
            {
                "A": 2,
                "x": [
                    {"B": 10, "__D__": 30},
                    {"B": 11, "__D__": 30},
                ],
                "c": ["62.0", "65.0"],
            },
        ]}
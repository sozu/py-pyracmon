from pyracmon.graph.spec import GraphSpec
import pytest
from dataclasses import dataclass
from typing import Generic, TypeVar, get_type_hints
from inspect import signature, Signature
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.graph import Graph, Node
from pyracmon.graph.identify import HierarchicalPolicy
from pyracmon.graph.serialize import *
from pyracmon.graph.schema import Typeable, issubgeneric
from pyracmon.graph.typing import TypedDict


template = GraphTemplate([("x", object, None, None)])


def node(v: Any) -> Node:
    return Node(template.x, v, None, 0)


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

    def test_name(self):
        ns = NodeSerializer()
        ns.name("abc")

        assert ns.namer("a") == "abc"
        assert not ns.be_merged

    def test_merge(self):
        ns = NodeSerializer()
        ns.merge(lambda n: f"__{n}__")

        assert ns.namer("a") == "__a__"
        assert ns.be_merged


class TestAggregator:
    def test_no_aggregator(self):
        ns = NodeSerializer()
        a = ns.aggregator
        r = a([node(1), node(2), node(3)])

        assert not ns.be_singular
        assert signature(a).return_annotation == list[T] # type: ignore
        assert [n.entity for n in r] == [1, 2, 3] # type: ignore

    def test_fold(self):
        ns = NodeSerializer()
        def agg(vs: list[Node]) -> int:
            return vs[2].entity
        ns.fold(agg)
        a = ns.aggregator
        r = a([node(1), node(2), node(3)])

        assert ns.be_singular
        assert signature(a).return_annotation is int
        assert r == 3

    def test_fold_nosig(self):
        ns = NodeSerializer()
        def agg(vs):
            return vs[2].entity
        ns.fold(agg)
        a = ns.aggregator
        r = a([node(1), node(2), node(3)])

        assert ns.be_singular
        assert signature(a).return_annotation == T
        assert r == 3

    def test_select(self):
        ns = NodeSerializer()
        def agg(vs: list[Node]) -> list[Node]:
            return vs[0:2]
        ns.select(agg)
        a = ns.aggregator
        r = a([node(1), node(2), node(3)])

        assert not ns.be_singular
        assert signature(a).return_annotation == list[Node]
        assert [n.entity for n in r] == [1, 2] # type: ignore

    def test_select_nosig(self):
        ns = NodeSerializer()
        def agg(vs):
            return vs[0:2]
        ns.select(agg)
        a = ns.aggregator
        r = a([node(1), node(2), node(3)])

        assert not ns.be_singular
        assert signature(a).return_annotation == list[T] # type: ignore
        assert [n.entity for n in r] == [1, 2] # type: ignore

    def test_invalid_fold(self):
        ns = NodeSerializer()
        def agg(vs: list[Node]) -> list[Node]:
            return vs[0:2]

        with pytest.raises(ValueError):
            ns.fold(agg)

    def test_invalid_select(self):
        ns = NodeSerializer()
        def agg(vs: list[Node]) -> Node:
            return vs[0]

        with pytest.raises(ValueError):
            ns.select(agg) # type: ignore

    def test_at(self):
        ns = NodeSerializer()
        ns.at(1, 100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([node(1), node(2), node(3)]).entity == 2 # type: ignore
        assert a([node(1)]) == 100

    def test_head(self):
        ns = NodeSerializer()
        ns.head(100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([node(1), node(2), node(3)]).entity == 1 # type: ignore
        assert a([]) == 100

    def test_last(self):
        ns = NodeSerializer()
        ns.last(100)
        a = ns.aggregator

        assert ns.be_singular
        assert a([node(1), node(2), node(3)]).entity == 3 # type: ignore
        assert a([]) == 100


class TestEach:
    def _context(self, entity) -> NodeContext:
        t = GraphTemplate([
            ("a", int, None, None),
        ])
        return NodeContextFactory(SerializationContext({}, lambda t: []), [], {}).begin(Node(t.a, entity, None, 0), [])

    def test_no_serializer(self):
        s = NodeSerializer().serializer
        r = s(self._context(5))

        assert signature(s).return_annotation is Signature.empty
        assert r == 5

    def test_serializers(self):
        def f1(cxt) -> int:
            return cxt.value+1
        def f2(cxt) -> float:
            return cxt.serialize()*1.3
        def f3(cxt) -> str:
            return f"{cxt.value + cxt.serialize()}"

        s = NodeSerializer().each(f1).each(f2).each(f3).serializer
        r = s(self._context(5))

        assert signature(s).return_annotation is str
        assert r == "12.8"

    def test_partial_annotation(self):
        def f1(cxt):
            return cxt.value+1
        def f2(cxt) -> float:
            vv = cxt.serialize()
            return vv*1.3
        def f3(cxt):
            vv = cxt.serialize()
            return f"{cxt.value + vv}"

        s = NodeSerializer().each(f1).each(f2).each(f3).serializer
        r = s(self._context(5))

        assert signature(s).return_annotation is float
        assert r == "12.8"

    def test_generic(self):
        X = TypeVar("X")
        class G(Generic[X]):
            pass
        def f0(cxt):
            return cxt.value
        def f1(cxt) -> int:
            return cxt.value
        def f2(cxt) -> G[T]:
            return cxt.value
        def f3(cxt) -> G[T]:
            return cxt.value

        s = NodeSerializer().each(f0).each(f1).each(f2).each(f3).serializer
        r = s(self._context(5))

        assert signature(s).return_annotation == G[G[int]]
        assert r == 5


class TestSub:
    def _template(self):
        t = GraphTemplate([
            ("a", dict, None, None),
            ("b", dict, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        t.a << [t.d >> t.b, t.c]

        u = GraphTemplate([
            ("a", int, None, None),
            ("t", t, None, None),
        ])

        return t, u

    def test_sub(self):
        t, u = self._template()

        sub = Graph(t).append(
            a=dict(a0=0, a1=1), b=dict(b0=10, b1=11), c=20, d=30,
        ).append(
            a=dict(a0=2, a1=3), b=dict(b0=12, b1=13), c=21, d=31,
        )

        ns = NodeSerializer().sub(a=S.of(), b=S.of(), c=S.of(), d=S.of())
        s = ns.serializer
        r = s(NodeContextFactory(SerializationContext({}, lambda t: []), [], {}).begin(Node(u.t, sub, None, 0), []))

        assert ns.be_singular
        assert issubgeneric(signature(s).return_annotation, Typeable)
        assert r == {
            "a": [
                {
                    "a0": 0, "a1": 1,
                    "b": [{"b0": 10, "b1": 11, "d": [30]}],
                    "c": [20],
                },
                {
                    "a0": 2, "a1": 3,
                    "b": [{ "b0": 12, "b1": 13, "d": [31]}],
                    "c": [21],
                },
            ]
        }


class TestAlter:
    class Base(TypedDict):
        a: int
        b: int
        c: int
        d: int
        e: int
    class Gen(TypedDict):
        g1: int
        g2: str

    def _context(self, entity):
        t = GraphTemplate([
            ("a", str, None, None),
        ])
        return NodeContextFactory(SerializationContext({}, lambda t: []), [], {}).begin(Node(t.a, entity, None, 0), [])

    def test_excludes(self):
        def f(cxt) -> TestAlter.Base:
            return TestAlter.Base(**{c:i for i, c in enumerate(cxt.value)})
        def gen(cxt) -> TestAlter.Gen:
            return TestAlter.Gen(g1=10, g2="def")

        s = NodeSerializer().each(f).alter(gen, ["b", "c"]).serializer
        r = s(self._context("abcde"))

        assert get_type_hints(Typeable.resolve(signature(s).return_annotation, self.Base, GraphSpec())) \
            == {"a": int, "d": int, "e": int, "g1": int, "g2": str}
        assert r == {"a": 0, "d": 3, "e": 4, "g1": 10, "g2": "def"}

    def test_includes(self):
        def f(cxt) -> TestAlter.Base:
            return TestAlter.Base(**{c:i for i, c in enumerate(cxt.value)})
        def gen(cxt) -> TestAlter.Gen:
            return TestAlter.Gen(g1=10, g2="def")

        s = NodeSerializer().each(f).alter(gen, includes=["b", "c", "g2"]).serializer
        r = s(self._context("abcde"))

        assert get_type_hints(Typeable.resolve(signature(s).return_annotation, self.Base, GraphSpec())) \
            == {"b": int, "c": int, "g2": str}
        assert r == {"b": 1, "c": 2, "g2": "def"}


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

    # TODO test_reuse

    def test_no_serializer(self):
        cxt = SerializationContext(dict(), lambda t:[])
        r = cxt.execute(self._graph())

        assert r == {}

    def test_default(self):
        cxt = SerializationContext(dict(a=S.of(), b=S.of(), c=S.of(), d=S.of()), lambda t:[])
        r = cxt.execute(self._graph())

        assert r == {"a": [0, 1, 2]}

    def test_finder(self):
        finder = lambda t: [lambda cxt: cxt.value*2]
        cxt = SerializationContext(dict(a=S.of(), b=S.of(), c=S.of(), d=S.of()), finder)
        r = cxt.execute(self._graph())

        assert r == {"a": [0, 2, 4]}

    def test_child(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value}),
            b=S.each(lambda cxt: {"B": cxt.value}),
            d=S.of(),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        print(r)

        assert r == {"a": [
            {"A": 0, "b": [{"B": 10, "d": [30, 31]}]},
            {"A": 1, "b": [{"B": 11, "d": [30]}, {"B": 12, "d": [30]}]},
            {"A": 2, "b": [{"B": 10, "d": [30]}, {"B": 11, "d": [30]}]},
        ]}

    def test_child_skipped(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value}),
            d=S.of(),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0}, {"A": 1}, {"A": 2},
        ]}

    def test_name(self):
        cxt = SerializationContext(dict(a=S.name("A")), lambda t:[])
        r = cxt.execute(self._graph())

        assert r == {"A": [0, 1, 2]}

    def test_merge(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value}),
            b=S.each(lambda cxt: {"B": cxt.value}).merge(),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0, "B": 10},
            {"A": 1, "B": 11},
            {"A": 2, "B": 10},
        ]}

    def test_merge_empty(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value}),
            b=S.each(lambda cxt: {"B": cxt.value}).merge(),
        ), lambda t: [])
        graph = Graph(self._template())
        graph.append(a=0, c=20, d=30)
        r = cxt.execute(graph.view)

        assert r == {"a": [
            {"A": 0},
        ]}

    def test_merge_named(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value}),
            b=S.each(lambda cxt: {"B": cxt.value}).merge(lambda n:f"__{n}__"),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0, "__B__": 10},
            {"A": 1, "__B__": 11},
            {"A": 2, "__B__": 10},
        ]}

    def test_merge_root(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"a1": cxt.value, "a2": cxt.value+1}).merge(),
            b=S.each(lambda cxt: {"B": cxt.value}).merge(lambda n:f"__{n}__"),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {
            "a1": 0, "a2": 1, "__B__": 10,
        }

    def test_fold(self):
        cxt = SerializationContext(dict(a=S.head()), lambda t:[])
        r = cxt.execute(self._graph())

        assert r == {"a": 0}

    def test_fold_alt(self):
        cxt = SerializationContext(dict(a=S.head("alt")), lambda t:[])
        r = cxt.execute(Graph(self._template()).view)

        assert r == {"a": "alt"}

    def test_alter_extend(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value, "B": cxt.value+1, "C": cxt.value+2}).alter(lambda cxt: {"D": cxt.value*3}),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0, "B": 1, "C": 2, "D": 0},
            {"A": 1, "B": 2, "C": 3, "D": 3},
            {"A": 2, "B": 3, "C": 4, "D": 6},
        ]}

    def test_alter_dataclass(self):
        @dataclass
        class DT:
            D: int

        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value, "B": cxt.value+1, "C": cxt.value+2}).alter(lambda cxt: DT(cxt.value*3)),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0, "B": 1, "C": 2, "D": 0},
            {"A": 1, "B": 2, "C": 3, "D": 3},
            {"A": 2, "B": 3, "C": 4, "D": 6},
        ]}

    def test_alter_shrink(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value, "B": cxt.value+1, "C": cxt.value+2}).alter(excludes=["B"]),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0, "C": 2},
            {"A": 1, "C": 3},
            {"A": 2, "C": 4},
        ]}

    def test_alter_includes(self):
        cxt = SerializationContext(dict(
            a=S.each(lambda cxt: {"A": cxt.value, "B": cxt.value+1, "C": cxt.value+2}).alter(excludes=["B"], includes=["A"]),
        ), lambda t: [])
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {"A": 0},
            {"A": 1},
            {"A": 2},
        ]}

    def test_node_params(self):
        def f(cxt):
            return cxt.value * cxt.params.v

        cxt = SerializationContext(dict(
            a=S.each(f).each(lambda cxt: {"A": cxt.serialize()}),
            c=S.each(f)
        ), lambda t: [], dict(
            a=dict(v=2), b=dict(v=10), c=dict(v=3),
        ))
        r = cxt.execute(self._graph())

        assert r == {
            "a": [
                {"A": 0, "c": [60, 63]},
                {"A": 2, "c": [60]},
                {"A": 4, "c": [60, 63]},
            ]
        }

    def test_all(self):
        def f1(cxt):
            return cxt.value+1
        def f2(cxt) -> float:
            vv = cxt.serialize()
            return vv*cxt.params.v
        def f3(cxt):
            vv = cxt.serialize()
            return f"{cxt.value + vv + cxt.params.w}"

        cxt = SerializationContext(dict(
            a = S.each(lambda c: {"A": c.value}),
            b = S.name("x").each(lambda c: {"B": c.value}),
            c = S.each(f1).each(f2).each(f3),
            d = S.last().merge(lambda n:f"__{n}__").each(lambda c: {"D": c.value}),
        ), lambda t: [], dict(
            c=dict(v=2.0, w=3.0),
        ))
        r = cxt.execute(self._graph())

        assert r == {"a": [
            {
                "A": 0,
                "x": [
                    {"B": 10, "__D__": 31},
                ],
                "c": ["65.0", "68.0"],
            },
            {
                "A": 1,
                "x": [
                    {"B": 11, "__D__": 30},
                    {"B": 12, "__D__": 30},
                ],
                "c": ["65.0"],
            },
            {
                "A": 2,
                "x": [
                    {"B": 10, "__D__": 30},
                    {"B": 11, "__D__": 30},
                ],
                "c": ["65.0", "68.0"],
            },
        ]}
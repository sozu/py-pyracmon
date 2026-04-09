import pytest
from dataclasses import dataclass
from typing import Annotated, Generic, TypedDict, TypeVar
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.graph import new_graph
from pyracmon.graph.serialize import S
from pyracmon.graph.schema import *
from pyracmon.graph.typing import walk_schema, DynamicType


T = TypeVar('T')


class TestTypedDict:
    def test_walk(self):
        class TDD(TypedDict):
            d1: int
        class TDC(TypedDict):
            c1: int
            d: TDD
        class TDB(TypedDict):
            b1: int
        class TDA(TypedDict):
            a1: int
            a2: str
            b: TDB
            c: TDC

        assert walk_schema(TDA) == {
            "a1": int, "a2": str,
            "b": {"b1": int},
            "c": {"c1": int, "d": {"d1": int}},
        }


class TestTypeable:
    def test_resolve(self):
        class A(Typeable[T]):
            @staticmethod
            def resolve(a, bound, arg, spec):
                return (bound,)

        assert Typeable.resolve(A[T], int, GraphSpec()) == (int,) # type: ignore

    def test_fixed(self):
        class A(Typeable[T]):
            @staticmethod
            def resolve(a, bound, arg, spec):
                return (bound,)

        assert Typeable.resolve(A[str], int, GraphSpec()) == (str,)

    def test_nest(self):
        class A(Typeable[T]):
            @staticmethod
            def resolve(a, bound, arg, spec):
                return (bound,)
        class B(Typeable[T]):
            @staticmethod
            def resolve(b, bound, arg, spec):
                return (bound,arg)

        assert Typeable.resolve(B[A[T]], int, GraphSpec()) == ((int,),int) # type: ignore


class TestAlter:
    def test_shrink(self):
        spec = GraphSpec()

        class TD(TypedDict):
            v1: int
            v2: str
            v3: float

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.alter(None, ["v2"]),
        )

        assert walk_schema(gs.schema) == {"a": [{"v1": int, "v3": float}]}

    def test_extend(self):
        spec = GraphSpec()

        class TD(TypedDict):
            v1: int
            v2: str
            v3: float
        class EX(TypedDict):
            v4: int
            v5: str
        def ext(v) -> EX:
            return EX(v4=0, v5="")

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.alter(ext),
        )

        assert walk_schema(gs.schema) == {"a": [{"v1": int, "v2": str, "v3": float, "v4": int, "v5": str}]}

    def test_extend_dataclass(self):
        spec = GraphSpec()

        @dataclass
        class TD:
            v1: int
            v2: str
            v3: float
        @dataclass
        class EX:
            v4: int
            v5: str
        def ext(v) -> EX:
            return EX(v4=0, v5="")

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.alter(ext),
        )

        assert walk_schema(gs.schema) == {"a": [{"v1": int, "v2": str, "v3": float, "v4": int, "v5": str}]}

    def test_composite(self):
        spec = GraphSpec()

        class TD(TypedDict):
            v1: int
            v2: str
            v3: float
        class EX1(TypedDict):
            v4: int
            v5: str
        class EX2(TypedDict):
            v5: float
        def ext1(v) -> EX1:
            return EX1(v4=0, v5="")
        def ext2(v) -> EX2:
            return EX2(v5=0)

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.alter(ext1, ["v2"]).alter(ext2, ["v3"]),
        )

        assert walk_schema(gs.schema) == {"a": [{"v1": int, "v4": int, "v5": float}]}


class TestSubGraph:
    def test_sub(self):
        spec = GraphSpec()

        class Base(TypedDict):
            v1: int
            v2: str
            v3: float
        class Sub(TypedDict):
            v4: int
            v5: str

        sub = spec.new_template(
            c = Sub,
            d = int,
        )
        sub.c << sub.d

        base = spec.new_template(
            a = Base,
            b = sub,
        )
        base.a << base.b

        gs = GraphSchema(
            spec, base,
            a = S.head(),
            b = S.sub(
                c = S.head(),
                d = S.of()
            )
        )

        assert walk_schema(gs.schema) == {
            "a": {
                "v1": int,
                "v2": str,
                "v3": float,
                "b": {
                    "c": {
                        "v4": int,
                        "v5": str,
                        "d": [int],
                    },
                },
            },
        }


class TestGraphSchema:
    def test_schema(self):
        class TDA(TypedDict):
            a1: int
            a2: Annotated[str, "A2"]
        class TDC(TypedDict):
            c1: Annotated[int, "C1"]
            c2: str
        class TDD(TypedDict):
            d1: int

        spec = GraphSpec()

        t = spec.new_template(
            a = TDA,
            b = str,
            c = TDC,
            d = TDD,
        )
        t.a << [t.b, t.d >> t.c]

        schema = GraphSchema(
            spec, t,
            a = S.doc("A"),
            b = S.doc("B").head(),
            c = S.doc("C").name("__c__"),
            d = S.doc("D").merge(lambda n:f"__{n}__"),
        )

        assert walk_schema(schema.schema) == {
            "a": [
                {
                    "a1": int, "a2": str,
                    "b": Optional[str],
                    "__c__": [
                        {
                            "c1": int, "c2": str,
                            "__d1__": int,
                        }
                    ],
                },
            ],
        }
        assert walk_schema(schema.schema, True) == {
            "a": ([
                {
                    "a1": (int, ""), "a2": (str, "A2"),
                    "b": (Optional[str], "B"),
                    "__c__": ([
                        {
                            "c1": (int, "C1"), "c2": (str, ""),
                            "__d1__": (int, ""),
                        }
                    ], "C"),
                },
            ], "A"),
        }

    def test_merge_root(self):
        class TDA(TypedDict):
            a1: int
            a2: Annotated[str, "A2"]
        class TDC(TypedDict):
            c1: Annotated[int, "C1"]
            c2: str
        class TDD(TypedDict):
            d1: int

        spec = GraphSpec()

        t = spec.new_template(
            a = TDA,
            b = str,
            c = TDC,
            d = TDD,
        )
        t.a << [t.b, t.d >> t.c]

        schema = GraphSchema(
            spec, t,
            a = S.doc("A").merge(),
            b = S.doc("B").head(),
            c = S.doc("C").name("__c__"),
            d = S.doc("D").merge(lambda n:f"__{n}__"),
        )

        assert walk_schema(schema.schema) == {
            "a1": int, "a2": str,
            "b": Optional[str],
            "__c__": [
                {
                    "c1": int, "c2": str,
                    "__d1__": int,
                }
            ],
        }


class TestSerializer:
    def test_empty(self):
        class TD(TypedDict):
            v: int

        spec = GraphSpec()

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A")).schema, True) == {"a": ([{"v": (int, "")}], "A")}

    def test_untyped_base(self):
        class TD(TypedDict):
            v: int

        spec = GraphSpec()
        spec.add_serializer(TD, lambda x:x)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A")).schema, True) == {"a": ([{"v": (int, "")}], "A")}

    def test_typed_base(self):
        class TD(TypedDict):
            v: int
        class TD2(TypedDict):
            u: str

        spec = GraphSpec()
        def base(x) -> TD2:
            return TD2(u=str(x.v))
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A")).schema, True) == {"a": ([{"u": (str, "")}], "A")}

    def test_generic_base(self):
        T = TypeVar("T")
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    u: Annotated[t, "U"] # type: ignore
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return TD2()
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A")).schema, True) == {"a": ([{"u": (int, "U")}], "A")}

    def test_untyped_serializer(self):
        class TD(TypedDict):
            v: int

        spec = GraphSpec()

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(lambda x:x)).schema, True) == {"a": ([{"v": (int, "")}], "A")}

    def test_typed_serializer(self):
        class TD(TypedDict):
            v: int
        class TD2(TypedDict):
            u: str

        def ser(x) -> TD2:
            return TD2(u=str(x.v))

        spec = GraphSpec()

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser)).schema, True) == {"a": ([{"u": (str, "")}], "A")}

    def test_resolved_at_typed(self):
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                class Schema(TypedDict):
                    u: Annotated[bound, "U"] # type: ignore
                return Schema

        def ser0(x) -> int:
            return 0
        def ser1(x) -> str:
            return ""
        def ser2(x) -> TD2[T]:
            return TD2()
        def ser3(x) -> DynamicType[T]:
            return DynamicType()

        spec = GraphSpec()

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1).each(ser2).each(ser3)).schema, True) \
            == {"a": ([{"u": (str, "U")}], "A")}

    def test_skip_untyped(self):
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                class Schema(TypedDict):
                    u: Annotated[bound, "U"] # type: ignore
                return Schema

        def ser0(x) -> int:
            return 0
        def ser1(x) -> str:
            return ""
        def ser2(x):
            return None
        def ser3(x) -> TD2[T]:
            return TD2()

        spec = GraphSpec()

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1).each(ser2).each(ser3)).schema, True) \
            == {"a": ([{"u": (str, "U")}], "A")}

    def test_untyped_base_untyped_serializer(self):
        class TD(TypedDict):
            v: int

        spec = GraphSpec()
        spec.add_serializer(TD, lambda x:x)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(lambda x:x)).schema, True) == {"a": ([{"v": (int, "")}], "A")}

    def test_typed_base_untyped_serializer(self):
        class TD(TypedDict):
            v: int
        class TD2(TypedDict):
            u: str

        spec = GraphSpec()
        def base(x) -> TD2:
            return TD2(u=str(x.v))
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(lambda x:x)).schema, True) == {"a": ([{"u": (str, "")}], "A")}

    def test_generic_base_untyped_serializer(self):
        T = TypeVar("T")
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    u: Annotated[t, "U"] # type: ignore
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return TD2()
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(lambda x:x)).schema, True) == {"a": ([{"u": (int, "U")}], "A")}

    def test_untyped_base_typed_serializer(self):
        class TD(TypedDict):
            v: int
        class TD3(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                class Schema(TypedDict):
                    u: Annotated[bound, "U"] # type: ignore
                return Schema

        spec = GraphSpec()
        spec.add_serializer(TD, lambda x:x)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> int:
            return 0
        def ser1(x) -> TD3[T]:
            return TD3()
        def ser2(x) -> DynamicType[T]:
            return DynamicType()

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1).each(ser2)).schema, True) == {"a": ([{"u": (int, "U")}], "A")}

    def test_typed_base_generic_serializer(self):
        class TD(TypedDict):
            v: int
        class TD2(TypedDict):
            w: str
        class TD3(Typeable[T]):
            @staticmethod
            def resolve(td3, bound, arg, spec):
                t = bound.__annotations__["w"]
                class Schema(TypedDict):
                    u: Annotated[t, "U"] # type: ignore
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2:
            return TD2(w=str(x.v))
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> TD3[T]:
            return TD3()
        def ser1(x) -> DynamicType[T]:
            return DynamicType()

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1)).schema, True) == {"a": ([{"u": (str, "U")}], "A")}

    def test_generic_base_generic_serializer(self):
        T = TypeVar("T")
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg, spec):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    w: Annotated[t, "W"] # type: ignore
                return Schema
        class TD3(Typeable[T]):
            @staticmethod
            def resolve(td3, bound, arg, spec):
                t = bound.__annotations__["w"]
                class Schema(TypedDict):
                    u: Annotated[t, "U"] # type: ignore
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return TD2()
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> TD3[T]:
            return TD3()
        def ser1(x) -> DynamicType[T]:
            return DynamicType()

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1)).schema, True) == {"a": ([{"u": (int, "U")}], "A")}


class TestSerialize:
    def test_serialize(self):
        spec = GraphSpec().add_identifier(int, lambda x:x)

        t = spec.new_template(a = int, b = int, c = int, d = int)
        t.a << [t.d >> t.b, t.c]

        graph = new_graph(t)

        graph.append(a=0, b=10, c=20, d=30)
        graph.append(a=0, b=10, c=21, d=31)
        graph.append(a=1, b=11, c=20, d=30)
        graph.append(a=1, b=12, c=20, d=30)
        graph.append(a=2, b=10, c=20, d=30)
        graph.append(a=2, b=11, c=21, d=30)

        def ser(cxt):
            return cxt.value * cxt.params.v

        gs = GraphSchema(
            spec, t,
            a = S.each(ser).each(lambda c: {"A": c.serialize()}),
            c = S.each(ser),
        )

        r = gs.serialize(graph.view, a = dict(v = 2), b = dict(v = 10), c = dict(v = 3))

        assert r == {
            "a": [
                {"A": 0, "c": [60, 63]},
                {"A": 2, "c": [60]},
                {"A": 4, "c": [60, 63]},
            ]
        }

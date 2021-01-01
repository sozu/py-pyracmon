import pytest
from typing import Generic, TypeVar
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.serialize import S
from pyracmon.graph.schema import *


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
            def resolve(a, bound, arg):
                return (bound,)

        assert Typeable.resolve(A[T], int) == (int,)

    def test_fixed(self):
        class A(Typeable[T]):
            @staticmethod
            def resolve(a, bound, arg):
                return (bound,)

        assert Typeable.resolve(A[str], int) == (str,)

    def test_nest(self):
        class A(Typeable[T]):
            @staticmethod
            def resolve(a, bound, arg):
                return (bound,)
        class B(Typeable[T]):
            @staticmethod
            def resolve(b, bound, arg):
                return (bound,arg)

        assert Typeable.resolve(B[A[T]], int) == ((int,),int)


class TestFix:
    def test_shrink(self):
        spec = GraphSpec()

        class TD(TypedDict):
            v1: int
            v2: str
            v3: float

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.fix(None, ["v2"]),
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
            return None

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.fix(ext),
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
            return None
        def ext2(v) -> EX2:
            return None

        t = spec.new_template(a=TD)

        gs = GraphSchema(
            spec, t,
            a = S.fix(ext1, ["v2"]).fix(ext2, ["v3"]),
        )

        assert walk_schema(gs.schema) == {"a": [{"v1": int, "v4": int, "v5": float}]}


class TestGraphSchema:
    def test_schema(self):
        class TDA(TypedDict):
            a1: int
            a2: document_type(str, "A2")
        class TDC(TypedDict):
            c1: document_type(int, "C1")
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
                    "b": str,
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
                    "b": (str, "B"),
                    "__c__": ([
                        {
                            "c1": (int, "C1"), "c2": (str, ""),
                            "__d1__": (int, ""),
                        }
                    ], "C"),
                },
            ], "A"),
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
            def resolve(td2, bound, arg):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    u: document_type(t, "U")
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return None
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
            def resolve(td2, bound, arg):
                class Schema(TypedDict):
                    u: document_type(bound, "U")
                return Schema

        def ser0(x) -> int:
            return None
        def ser1(x) -> str:
            return None
        def ser2(x) -> TD2[T]:
            return None
        def ser3(x) -> DynamicType[T]:
            return None

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
            def resolve(td2, bound, arg):
                class Schema(TypedDict):
                    u: document_type(bound, "U")
                return Schema

        def ser0(x) -> int:
            return None
        def ser1(x) -> str:
            return None
        def ser2(x):
            return None
        def ser3(x) -> TD2[T]:
            return None

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
            def resolve(td2, bound, arg):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    u: document_type(t, "U")
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return None
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
            def resolve(td2, bound, arg):
                class Schema(TypedDict):
                    u: document_type(bound, "U")
                return Schema

        spec = GraphSpec()
        spec.add_serializer(TD, lambda x:x)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> int:
            return None
        def ser1(x) -> TD3[T]:
            return None
        def ser2(x) -> DynamicType[T]:
            return None

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1).each(ser2)).schema, True) == {"a": ([{"u": (int, "U")}], "A")}

    def test_typed_base_generic_serializer(self):
        class TD(TypedDict):
            v: int
        class TD2(TypedDict):
            w: str
        class TD3(Typeable[T]):
            @staticmethod
            def resolve(td3, bound, arg):
                t = bound.__annotations__["w"]
                class Schema(TypedDict):
                    u: document_type(t, "U")
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2:
            return TD2(w=str(x.v))
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> TD3[T]:
            return None
        def ser1(x) -> DynamicType[T]:
            return None

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1)).schema, True) == {"a": ([{"u": (str, "U")}], "A")}

    def test_generic_base_generic_serializer(self):
        T = TypeVar("T")
        class TD(TypedDict):
            v: int
        class TD2(Typeable[T]):
            @staticmethod
            def resolve(td2, bound, arg):
                t = bound.__annotations__["v"]
                class Schema(TypedDict):
                    w: document_type(t, "W")
                return Schema
        class TD3(Typeable[T]):
            @staticmethod
            def resolve(td3, bound, arg):
                t = bound.__annotations__["w"]
                class Schema(TypedDict):
                    u: document_type(t, "U")
                return Schema

        spec = GraphSpec()
        def base(x) -> TD2[T]:
            return None
        spec.add_serializer(TD, base)

        t = spec.new_template(
            a = TD,
        )

        def ser0(x) -> TD3[T]:
            return None
        def ser1(x) -> DynamicType[T]:
            return None

        assert walk_schema(spec.to_schema(t, a=S.doc("A").each(ser0).each(ser1)).schema, True) == {"a": ([{"u": (int, "U")}], "A")}


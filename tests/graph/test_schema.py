import pytest
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.serialize import S
from pyracmon.graph.schema import *


class TestTypedDict:
    def test_extend(self):
        class TD(TypedDict):
            a: int

        extend_dict(TD, b=str, c=float)

        assert TD.__annotations__ == {"a":int, "b":str, "c":float}

    def test_shrink(self):
        class TD(TypedDict):
            a: int
            b: str
            c: float

        shrink_dict(TD, "b", "c")

        assert TD.__annotations__ == {"a":int}

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

        assert walk_dict(TDA) == {
            "a1": int, "a2": str,
            "b": {"b1": int},
            "c": {"c1": int, "d": {"d1": int}},
        }


class TestGraphSchema:
    def test_schema(self):
        class TDA(TypedDict):
            a1: int
            a2: str
        class TDC(TypedDict):
            c1: int
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
            a = S.of(),
            b = S.head(),
            c = S.name("__c__"),
            d = S.merge(lambda n:f"__{n}__"),
        )

        assert walk_dict(schema.schema) == {
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
            ]
        }

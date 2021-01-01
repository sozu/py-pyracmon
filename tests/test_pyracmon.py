import sys
import psycopg2
import pytest
from tests import models as m
from pyracmon import *
from pyracmon.graph.schema import *
from pyracmon.dialect import postgresql


def _connect():
    return connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )


tables = ["t1", "t2", "t3", "t4", "v1", "mv1", "mv2", "types"]


class TestDeclareModels:
    def test_module_name_postgresql(self):
        db = _connect()

        declare_models(postgresql, db, 'tests.models')

        try:
            for t in tables:
                assert hasattr(m, t)
        finally:
            for t in tables:
                del sys.modules['tests.models'].__dict__[t]

    def test_module_obj_postgresql(self):
        db = _connect()

        declare_models(postgresql, db, m)

        try:
            for t in tables:
                assert hasattr(m, t)
        finally:
            for t in tables:
                del sys.modules['tests.models'].__dict__[t]


class TestModelGraph:
    def test_graph(self):
        db = _connect()

        declare_models(postgresql, db, m)

        template = graph_template(
            t1 = m.t1,
            t2 = m.t2,
            t3 = m.t3,
            num = int,
        )
        template.t1 << [template.num >> template.t2, template.t3]

        graph = new_graph(template)

        graph.append(
            t1 = m.t1(c11=1, c12=11),
            t2 = m.t2(c21=1, c22=21),
            t3 = m.t3(c31=1, c32=31),
            num = 0,
        )
        graph.append(
            t1 = m.t1(c11=1, c12=111),
            t2 = m.t2(c21=1, c22=211),
            t3 = m.t3(c31=2, c32=32),
            num = 1,
        )
        graph.append(
            t1 = m.t1(c11=1, c12=111),
            t2 = m.t2(c21=2, c22=22),
            t3 = m.t3(c31=2, c32=33),
            num = 2,
        )
        graph.append(
            t1 = m.t1(c11=None),
            t2 = m.t2(c21=3, c22=21),
            t3 = m.t3(c31=4, c32=31),
            num = 3,
        )
        graph.append(
            t1 = m.t1(c11=2),
            t2 = m.t2(c21=1, c22=21),
            t3 = m.t3(c31=None),
            num = 4,
        )
        graph.append(
            t1 = m.t1(c11=2, c12=12),
            t2 = m.t2(c21=1, c22=21),
            t3 = m.t3(c31=5, c32=None),
            num = 5,
        )
        graph.append(
            t2 = m.t2(c21=1, c22=21),
            num = 6,
        )

        view = graph.view

        assert [(n(), [(n2(), [v() for v in n2.num]) for n2 in n.t2], [n3() for n3 in n.t3]) for n in view.t1] \
            == [
                (
                    m.t1(c11=1, c12=11),
                    [(m.t2(c21=1, c22=21), [0, 6]), (m.t2(c21=1, c22=211), [1]), (m.t2(c21=2, c22=22), [2])],
                    [m.t3(c31=1, c32=31), m.t3(c31=2, c32=32)],
                ),
                (
                    m.t1(c11=2),
                    [(m.t2(c21=1, c22=21), [4, 5, 6])],
                    [m.t3(c31=5, c32=None)],
                ),
            ]

        r = graph_dict(
            view,
            t1 = S.of(),
            t2 = S.of(),
            t3 = S.of(),
            num = S.of(),
        )

        assert r == {
            "t1": [
                {
                    "c11": 1, "c12": 11,
                    "t2": [{"c21": 1, "c22": 21, "num": [0, 6]}, {"c21": 1, "c22": 211, "num": [1]}, {"c21": 2, "c22": 22, "num": [2]}],
                    "t3": [{"c32": 31}, {"c32": 32}],
                },
                {
                    "c11": 2,
                    "t2": [{"c21": 1, "c22": 21, "num": [4, 5, 6]}],
                    "t3": [{"c32": None}],
                }
            ]
        }

        gs = graph_schema(
            template,
            t1 = S.doc("T1"),
            t2 = S.of(),
            t3 = S.of(),
            num = S.doc("Num"),
        )

        assert r == gs.serialize(view)
        assert walk_schema(gs.schema, True) == {
            "t1": ([
                {
                    "c11": (int, "comment of c11"), "c12": (int, "comment of c12"), "c13": (str, "comment of c13"),
                    "t2": ([
                        {
                            "c21": (int, ""), "c22": (int, ""), "c23": (str, ""),
                            "num": ([int], "Num"),
                        },
                    ], ""),
                    "t3": ([
                        {
                            "c32": (int, ""), "c33": (str, ""),
                        }
                    ], ""),
                },
            ], "T1"),
        }
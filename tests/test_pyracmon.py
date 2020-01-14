import sys
import psycopg2
import pytest
from tests import models as m
from pyracmon import declare_models, graph_template, graph_dict, CRUDMixin, GraphEntityMixin, add_identifier, add_serializer, add_entity_filter
from pyracmon.connection import connect
from pyracmon.dialect import postgresql
from pyracmon.model import define_model, Table, Column
from pyracmon.graph import Graph


def _connect():
    return connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )


def test_module_name_postgresql():
    db = _connect()

    declare_models(postgresql, db, 'tests.models')
    try:
        assert hasattr(m, "t1")
        assert hasattr(m, "t2")
        assert hasattr(m, "t3")
        assert hasattr(m, "t4")
    finally:
        del sys.modules['tests.models'].__dict__["t1"]
        del sys.modules['tests.models'].__dict__["t2"]
        del sys.modules['tests.models'].__dict__["t3"]
        del sys.modules['tests.models'].__dict__["t4"]


def test_module_obj_postgresql():
    db = _connect()

    declare_models(postgresql, db, m)
    try:
        assert hasattr(m, "t1")
        assert hasattr(m, "t2")
        assert hasattr(m, "t3")
        assert hasattr(m, "t4")
    finally:
        del sys.modules['tests.models'].__dict__["t1"]
        del sys.modules['tests.models'].__dict__["t2"]
        del sys.modules['tests.models'].__dict__["t3"]
        del sys.modules['tests.models'].__dict__["t4"]


def test_model_graph():
    t1 = Table("t1", [
        Column("c1", int, None, True, False, None),
        Column("c2", int, None, False, False, None),
    ])
    t2 = Table("t2", [
        Column("c1", int, None, True, False, None),
        Column("c2", int, None, False, False, None),
    ])

    m1 = define_model(t1, [CRUDMixin, GraphEntityMixin])
    m2 = define_model(t2, [CRUDMixin, GraphEntityMixin])

    t = graph_template(
        m1 = m1,
        m2 = m2,
    )
    t.m2 >> t.m1

    graph = Graph(t)

    graph.append(
        m1 = m1(c1 = 1, c2 = "a"),
        m2 = m2(c1 = 1, c2 = 1),
    )
    graph.append(
        m1 = m1(c1 = 2, c2 = "b"),
        m2 = m2(c1 = 2, c2 = 2),
    )
    graph.append(
        m1 = m1(c1 = 2, c2 = "dummy"),
        m2 = m2(c1 = 3, c2 = 2),
    )
    graph.append(
        m1 = m1(c1 = None, c2 = None),
        m2 = m2(c1 = 3, c2 = 2),
    )
    graph.append(
        m1 = m1(c1 = 3, c2 = "c"),
        m2 = m2(c1 = 1, c2 = 3),
    )
    graph.append(
        m1 = m1(c1 = 4, c2 = "d"),
        m2 = m2(c1 = None, c2 = None),
    )

    assert graph_dict(
        graph.view,
        m1 = (),
        m2 = (),
    ) == dict(
        m1 = [
            dict(c1 = 1, c2 = "a", m2 = [dict(c1 = 1, c2 = 1)]),
            dict(c1 = 2, c2 = "b", m2 = [dict(c1 = 2, c2 = 2), dict(c1 = 3, c2 = 2)]),
            dict(c1 = 3, c2 = "c", m2 = [dict(c1 = 1, c2 = 3)]),
            dict(c1 = 4, c2 = "d", m2 = []),
        ]
    )


def test_add_identifier():
    t1 = Table("t1", [
        Column("c1", int, None, True, False, None),
        Column("c2", int, None, False, False, None),
    ])

    m1 = define_model(t1, [CRUDMixin, GraphEntityMixin])

    add_identifier(m1, lambda x: x.c2)

    t = graph_template(m1 = m1)
    graph = Graph(t)

    graph.append(m1 = m1(c1 = 1, c2 = "a"))
    graph.append(m1 = m1(c1 = 2, c2 = "a"))

    assert len(graph.view.m1) == 1
    v = graph.view.m1[0]()
    assert v.c1 == 1
    assert v.c2 == "a"


def test_no_pk():
    t1 = Table("t1", [
        Column("c1", int, None, False, False, None),
        Column("c2", int, None, False, False, None),
    ])

    m1 = define_model(t1, [CRUDMixin, GraphEntityMixin])

    t = graph_template(m1 = m1)
    graph = Graph(t)

    graph.append(m1 = m1(c1 = 1, c2 = "a"))
    graph.append(m1 = m1(c1 = 1, c2 = "a"))

    assert len(graph.view.m1) == 2


def test_add_entity_filter():
    t1 = Table("t1", [
        Column("c1", int, None, True, False, None),
        Column("c2", int, None, False, False, None),
    ])

    m1 = define_model(t1, [CRUDMixin, GraphEntityMixin])

    add_entity_filter(m1, lambda x: x.c1 >= 0)

    t = graph_template(m1 = m1, v = int)
    t.m1 << t.v
    graph = Graph(t)

    graph.append(m1 = m1(c1 = -1, c2 = "a"), v = 1)
    graph.append(m1 = m1(c1 = 0, c2 = "b"), v = 2)
    graph.append(m1 = m1(c1 = 1, c2 = "c"), v = 3)

    assert [(n().c1, n().c2) for n in graph.view.m1] == [(0, "b"), (1, "c")]
    assert [n() for n in graph.view.v] == [2, 3]
    assert [n() for n in graph.view.m1[0].v] == [2]
    assert [n() for n in graph.view.m1[1].v] == [3]


def test_add_serializer():
    t1 = Table("t1", [
        Column("c1", int, None, True, False, None),
        Column("c2", int, None, False, False, None),
    ])

    m1 = define_model(t1, [CRUDMixin, GraphEntityMixin])

    t = graph_template(m1 = m1)
    graph = Graph(t)

    graph.append(m1 = m1(c1 = 1, c2 = "a"))

    add_serializer(m1, lambda s, x: dict(c1 = x.c1 * 2, c2 = f"__{x.c2}__"))

    assert graph_dict(graph.view, m1 = ()) == dict(m1 = [dict(c1 = 2, c2 = "__a__")])


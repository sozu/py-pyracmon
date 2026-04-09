import pytest
from collections.abc import Sequence
from typing import Any
from pyracmon.model import Model, Table, Column, define_model, COLUMN
from pyracmon.graph import GraphTemplate, new_graph
from pyracmon.select import Selection, SelectMixin
from pyracmon.query_graph import append_rows
from .db_api import PseudoCursor as Cursor


table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, False, None, None, False),
    Column("c3", int, None, False, None, None, True),
])
class T1(Model, SelectMixin): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN

table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, None, None, True),
    Column("c3", int, None, False, None, None, False),
])
class T2(Model, SelectMixin): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN


model1 = define_model(table1, [SelectMixin], model_type=T1)
model2 = define_model(table2, [SelectMixin], model_type=T2)


class PseudoCursor(Cursor):
    def __init__(self, rows: Sequence[Sequence[Any]]) -> None:
        self.rows = rows

    def fetchall(self):
        return self.rows


class TestAppendRows:
    def template(self) -> GraphTemplate:
        tmpl = GraphTemplate([
            ("a", model1, None, None),
            ("b", model2, None, None),
            ("c", int, None, None),
            ("d", int, None, None),
        ])
        tmpl.a << [tmpl.b, tmpl.d >> tmpl.c]
        return tmpl

    def test_empty(self):
        exp = model1.select("m1")

        graph = append_rows(
            PseudoCursor([]),
            exp,
            new_graph(self.template()),
            a=exp.m1,
        ).view

        assert [v() for v in graph.a] == []

    def test_single_table(self):
        exp = model1.select("m1")

        graph = append_rows(
            PseudoCursor([
                [1, 2, 3],
                [4, 5, 6],
            ]),
            exp,
            new_graph(self.template()),
            a=exp.m1,
        ).view

        assert [v() for v in graph.a] == [model1(c1=1, c2=2, c3=3), model1(c1=4, c2=5, c3=6)]

    def test_multi_tables(self):
        exp = model1.select("m1") + model2.select("m2") + "c"

        graph = append_rows(
            PseudoCursor([
                [1, 2, 3, 11, 12, 13, 7],
                [4, 5, 6, 14, 15, 16, 17],
            ]),
            exp,
            new_graph(self.template()),
            a=exp.m1, b=exp.m2, c=exp.c,
        ).view

        assert [v() for v in graph.a] == [model1(c1=1, c2=2, c3=3), model1(c1=4, c2=5, c3=6)]
        assert [v() for v in graph.b] == [model2(c1=11, c2=12, c3=13), model2(c1=14, c2=15, c3=16)]
        assert [v() for v in graph.c] == [7, 17]
        assert [v() for v in graph.d] == []

import pytest
from pyracmon.graph.graph import Graph, Node
from pyracmon.graph.serialize import S, NodeContextFactory
from pyracmon.graph.spec import *


class TestIdentifier:
    def test_find(self):
        identi = lambda x: x
        idents = lambda x: x

        spec = GraphSpec()
        spec.add_identifier(int, identi)
        spec.add_identifier(str, idents)

        assert spec.get_identifier(int) is identi
        assert spec.get_identifier(str) is idents
        assert spec.get_identifier(float) is None

    def test_inherit(self):
        class A:
            pass
        class B(A):
            pass
        ident = lambda x: x

        spec = GraphSpec()
        spec.add_identifier(A, ident)

        assert spec.get_identifier(B) is ident

    def test_priority(self):
        ident1 = lambda x:x
        ident2 = lambda x:x

        spec = GraphSpec()
        spec.add_identifier(int, ident1)
        spec.add_identifier(int, ident2)

        assert spec.get_identifier(int) is ident2


class TestEntityFilter:
    def test_find(self):
        ef1 = lambda x: True
        ef2 = lambda x: False

        spec = GraphSpec()
        spec.add_entity_filter(int, ef1)
        spec.add_entity_filter(str, ef2)

        assert spec.get_entity_filter(int) is ef1
        assert spec.get_entity_filter(str) is ef2
        assert spec.get_entity_filter(float) is None

    def test_inherit(self):
        class A:
            pass
        class B(A):
            pass
        ef = lambda x: x

        spec = GraphSpec()
        spec.add_entity_filter(A, ef)

        assert spec.get_entity_filter(B) is ef

    def test_priority(self):
        ef1 = lambda x:True
        ef2 = lambda x:True

        spec = GraphSpec()
        spec.add_entity_filter(int, ef1)
        spec.add_entity_filter(int, ef2)

        assert spec.get_entity_filter(int) is ef2


class TestSerializer:
    def test_find(self):
        ser1 = lambda x: x
        ser2 = lambda x: x

        spec = GraphSpec()
        spec.add_serializer(int, ser1)
        spec.add_serializer(str, ser2)

        assert spec.find_serializers(int) == [ser1]
        assert spec.find_serializers(str) == [ser2]
        assert spec.find_serializers(float) == []

    def test_inherit(self):
        class A:
            pass
        class B(A):
            pass
        ser = lambda x: x

        spec = GraphSpec()
        spec.add_serializer(A, ser)

        assert spec.find_serializers(B) == [ser]

    def test_multiple(self):
        class A:
            pass
        class B(A):
            pass
        ser1 = lambda x: x
        ser2 = lambda x: x

        spec = GraphSpec()
        spec.add_serializer(A, ser1)
        spec.add_serializer(B, ser2)

        assert spec.find_serializers(B) == [ser1, ser2]
        assert spec.find_serializers(A) == [ser1]

    def test_priority(self):
        ser1 = lambda x:x
        ser2 = lambda x:x

        spec = GraphSpec()
        spec.add_serializer(int, ser1)
        spec.add_serializer(int, ser2)

        assert spec.find_serializers(int) == [ser1, ser2]


class TestNewTemplate:
    def test_new(self):
        spec = GraphSpec()

        ident = lambda x:x
        ef = lambda x:True
        ser = lambda x:x

        spec.add_identifier(float, ident)
        spec.add_entity_filter(float, ef)
        spec.add_serializer(float, ser)

        efd = lambda s:True

        t = spec.new_template(
            a = (),
            b = (int,),
            c = float,
            d = (str, len, efd)
        )

        assert list(t) == [t.a, t.b, t.c, t.d]
        assert (t.a.name, t.a.kind, t.a.policy.identifier, t.a.entity_filter) == ("a", object, None, None)
        assert (t.b.name, t.b.kind, t.b.policy.identifier, t.b.entity_filter) == ("b", int, None, None)
        assert (t.c.name, t.c.kind, t.c.policy.identifier, t.c.entity_filter) == ("c", float, ident, ef)
        assert (t.d.name, t.d.kind, t.d.policy.identifier, t.d.entity_filter) == ("d", str, len, efd)

    def test_bases(self):
        spec = GraphSpec()

        efi = lambda x:True
        eff = lambda x:True

        spec.add_entity_filter(int, efi).add_entity_filter(float, eff)

        t1 = spec.new_template(a = int, b = float)
        t2 = spec.new_template(c = int, d = float)

        t = spec.new_template(t1, t2, e = int, f = float)

        assert list(t) == [t.a, t.b, t.c, t.d, t.e, t.f]
        assert (t.a.name, t.a.kind, t.a.entity_filter) == ("a", int, efi)
        assert (t.b.name, t.b.kind, t.b.entity_filter) == ("b", float, eff)
        assert (t.c.name, t.c.kind, t.c.entity_filter) == ("c", int, efi)
        assert (t.d.name, t.d.kind, t.d.entity_filter) == ("d", float, eff)
        assert (t.e.name, t.e.kind, t.e.entity_filter) == ("e", int, efi)
        assert (t.f.name, t.f.kind, t.f.entity_filter) == ("f", float, eff)


class TestToDict:
    class A:
        def __init__(self, v, w):
            self.v = v
            self.w = w

    def _template(self, spec):
        t = spec.new_template(
            a = dict,
            b = int,
            c = str,
            d = self.A,
        )
        t.a << [t.d >> t.b, t.c]

        return t

    def test_to_dict(self):
        spec = GraphSpec()

        spec.add_identifier(self.A, lambda x:x.v)
        spec.add_identifier(int, lambda x:x)
        spec.add_identifier(str, lambda x:x)
        spec.add_entity_filter(int, lambda x:x>0)
        spec.add_serializer(self.A, lambda c:dict(v=c.value.v, w=c.value.w))
        spec.add_serializer(int, lambda c:dict(i=c.value))

        t = spec.new_template(
            a = self.A,
            b = int,
            c = str,
            d = float,
        )
        t.a << [t.d >> t.b, t.c]

        graph = Graph(t)

        graph.append(a=self.A(0, 1), b=10, c="a", d=0.5)
        graph.append(a=self.A(0, 2), b=11, c="a", d=0.5)
        graph.append(a=self.A(0, 3), b=11, c="b", d=0.5)
        graph.append(a=self.A(1, 1), b=10, c="c", d=0.5)
        graph.append(a=self.A(2, 1), b=10, c="c", d=0.5)
        graph.append(a=self.A(2, 1), b= 0, c="d", d=0.5)

        assert spec.to_dict(
            graph.view,
            a = S.of(),
            b = S.of(),
            c = S.of(),
            d = S.of(),
        ) == {
            "a": [
                {
                    "v": 0, "w": 1,
                    "b": [
                        {
                            "i": 10,
                            "d": [0.5],
                        },
                        {
                            "i": 11,
                            "d": [0.5, 0.5],
                        },
                    ],
                    "c": ["a", "b"],
                },
                {
                    "v": 1, "w": 1,
                    "b": [
                        {
                            "i": 10,
                            "d": [0.5],
                        },
                    ],
                    "c": ["c"],
                },
                {
                    "v": 2, "w": 1,
                    "b": [
                        {
                            "i": 10,
                            "d": [0.5],
                        },
                    ],
                    "c": ["c", "d"],
                },
            ]
        }

    def test_sub_graph(self):
        spec = GraphSpec()

        spec.add_identifier(self.A, lambda x:x.v)
        spec.add_serializer(self.A, lambda c:dict(v=c.value.v, w=c.value.w))

        sub = spec.new_template(
            a = self.A,
            b = str,
            c = int,
        )
        sub.a << [sub.b, sub.c]

        t = spec.new_template(
            a = self.A,
            b = sub,
        )
        t.a << t.b

        graph = Graph(t)

        graph.append(a=self.A(0, 1), b=dict(a=self.A(10, 1), b="a", c=0.1))
        graph.append(a=self.A(0, 2), b=dict(a=self.A(10, 2), b="b", c=0.2))
        graph.append(a=self.A(0, 3), b=dict(a=self.A(11, 1), b="b", c=0.3))
        graph.append(a=self.A(1, 1), b=dict(a=self.A(10, 1), b="a", c=0.4))
        graph.append(a=self.A(2, 1), b=dict(a=self.A(10, 1)))

        assert spec.to_dict(
            graph.view,
            a = S.of(),
            b = S.sub(
                a = S.of(),
                b = S.of(),
                c = S.head(100),
            ),
        ) == {
            "a": [
                {
                    "v": 0, "w": 1,
                    "b": {
                        "a": [
                            {
                                "v": 10, "w": 1,
                                "b": ["a", "b"],
                                "c": 0.1,
                            },
                            {
                                "v": 11, "w": 1,
                                "b": ["b"],
                                "c": 0.3,
                            },
                        ],
                    },
                },
                {
                    "v": 1, "w": 1,
                    "b": {
                        "a": [
                            {
                                "v": 10, "w": 1,
                                "b": ["a"],
                                "c": 0.4,
                            },
                        ],
                    },
                },
                {
                    "v": 2, "w": 1,
                    "b": {
                        "a": [
                            {
                                "v": 10, "w": 1,
                                "b": [],
                                "c": 100,
                            },
                        ],
                    },
                },
            ],
        }
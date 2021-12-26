import pytest
import inspect
from pyracmon.model import Table, Column, Relations, define_model
from pyracmon.model_graph import *
from pyracmon.graph.graph import Node
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.schema import walk_schema, Typeable, TypedDict, document_type
from pyracmon.graph.serialize import chain_serializers, S, NodeContextFactory


table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False, "c1 in t1"),
    Column("c2", int, None, False, Relations(), None, False, "c2 in t1"),
    Column("c3", int, None, False, None, None, True, "c3 in t1"),
])


table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, Relations(), None, False),
    Column("c3", int, None, False, None, None, False),
])


table3 = Table("t3", [
    Column("c1", int, None, False, None, "seq", False),
    Column("c2", int, None, False, Relations(), None, False),
    Column("c3", int, None, False, None, None, False),
])


class TestConfigurableSpec:
    def test_create(self):
        spec = ConfigurableSpec.create()

        assert spec.get_identifier(GraphEntityMixin) is not None
        assert spec.get_entity_filter(GraphEntityMixin) is not None
        assert len(spec.find_serializers(GraphEntityMixin)) == 2

    def test_clone(self):
        spec = ConfigurableSpec.create()
        clone = spec.clone()

        clone.add_identifier(int, lambda x:x)
        clone.add_entity_filter(int, lambda x:True)
        clone.add_serializer(int, lambda x:x)

        assert (len(spec.identifiers), len(spec.entity_filters), len(spec.serializers)) == (1, 1, 1)
        assert (len(clone.identifiers), len(clone.entity_filters), len(clone.serializers)) == (2, 2, 2)

        clone.include_fk = True

        assert spec.get_identifier(int) is None
        assert spec.get_entity_filter(int) is None
        assert len(spec.find_serializers(int)) == 0
        assert spec.include_fk is False

        spec.replace(clone)

        assert (len(spec.identifiers), len(spec.entity_filters), len(spec.serializers)) == (2, 2, 2)
        assert spec.get_identifier(int) is not None
        assert spec.get_entity_filter(int) is not None
        assert len(spec.find_serializers(int)) == 1
        assert spec.include_fk is True


class TestIdentity:
    def test_pk(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=None, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_identifier(type(v))(v) == (1,)

    def test_not_set(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c2=2, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_identifier(type(v))(v) is None

    def test_pks(self):
        m = define_model(table2, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_identifier(type(v))(v) == (1, 2)

    def test_no_pk(self):
        m = define_model(table3, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_identifier(type(v))(v) is None


class TestNull:
    def test_all_none(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=None, c2=None, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_entity_filter(type(v))(v) is False

    def test_partial_none(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=None, c3=None)

        spec = ConfigurableSpec.create()

        assert spec.get_entity_filter(type(v))(v) is True

    def test_no_column(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m()

        spec = ConfigurableSpec.create()

        assert spec.get_entity_filter(type(v))(v) is False


class TestFK:
    def _context(self, model):
        t = GraphTemplate([
            ("a", type(model), None, None),
        ])
        return NodeContextFactory(None, [], {}).begin(Node(t.a, model, None, 0), [])

    def test_excludes(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()

        assert chain_serializers(spec.find_serializers(type(v)))(self._context(v)) == {"c1": 1, "c3": 3}

    def test_includes(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()
        spec.include_fk = True

        assert chain_serializers(spec.find_serializers(type(v)))(self._context(v)) == {"c1": 1, "c2": 2, "c3": 3}


class TestSchema:
    def test_schema(self):
        m = define_model(table1, [GraphEntityMixin])

        spec = ConfigurableSpec.create()

        s = chain_serializers(spec.find_serializers(m))

        rt = inspect.signature(s).return_annotation

        assert walk_schema(Typeable.resolve(rt, m, spec)) == {"c1": int, "c3": int}
        assert walk_schema(Typeable.resolve(rt, m, spec), True) == {"c1": (int, "c1 in t1"), "c3": (int, "c3 in t1")}

    def test_include_fk(self):
        m = define_model(table1, [GraphEntityMixin])

        spec = ConfigurableSpec.create()
        spec.include_fk = True

        s = chain_serializers(spec.find_serializers(m))

        rt = inspect.signature(s).return_annotation

        assert walk_schema(Typeable.resolve(rt, m, spec)) == {"c1": int, "c2": int, "c3": int}
        assert walk_schema(Typeable.resolve(rt, m, spec), True) == {"c1": (int, "c1 in t1"), "c2": (int, "c2 in t1"), "c3": (int, "c3 in t1")}

    def test_serializer(self):
        m = define_model(table1, [GraphEntityMixin])

        spec = ConfigurableSpec.create()
        spec.add_serializer(m, S.alter(excludes={"c3"}))

        s = chain_serializers(spec.find_serializers(m))

        rt = inspect.signature(s).return_annotation

        assert walk_schema(Typeable.resolve(rt, m, spec)) == {"c1": int}
        assert walk_schema(Typeable.resolve(rt, m, spec), True) == {"c1": (int, "c1 in t1")}

    def test_add_fk_schema(self):
        m = define_model(table1, [GraphEntityMixin])

        class Ex(TypedDict):
            c2: document_type(int, "fk")

        def ex(model: m) -> Ex:
            return Ex(c2 = m.c2)

        spec = ConfigurableSpec.create()
        spec.add_serializer(m, S.alter(ex, excludes={"c3"}))

        s = chain_serializers(spec.find_serializers(m))

        rt = inspect.signature(s).return_annotation

        assert walk_schema(Typeable.resolve(rt, m, spec)) == {"c1": int, "c2": int}
        assert walk_schema(Typeable.resolve(rt, m, spec), True) == {"c1": (int, "c1 in t1"), "c2": (int, "fk")}
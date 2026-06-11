import pytest
from copy import deepcopy
import inspect
from typing import Annotated, TypedDict
from pyracmon.model import define_model, Model
from pyracmon.graph.graph import Node
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.schema import Typeable
from pyracmon.graph.typing import walk_schema
from pyracmon.graph.serialize import chain_serializers, S, NodeContext, NodeContextFactory, SerializationContext
from pyracmon.model_graph import ConfigurableSpec, GraphEntityMixin, _serialize_model, _exclude_fk
from .fixtures import *


class T1(Model): c1: int = COLUMN; c2: int = COLUMN; c3: int = COLUMN


class TestConfigurableSpec:
    def test_create(self):
        spec = ConfigurableSpec.create()

        assert spec.get_identifier(GraphEntityMixin) is not None
        assert spec.get_entity_filter(GraphEntityMixin) is not None
        assert spec.find_serializers(GraphEntityMixin) == [_serialize_model, _exclude_fk]

    def test_deepcopy(self):
        spec = ConfigurableSpec.create()
        clone = deepcopy(spec)

        clone.add_identifier(int, lambda x:x)
        clone.add_entity_filter(int, lambda x:True)
        clone.add_serializer(int, lambda x:x)
        clone.include_fk = True

        assert (len(spec.identifiers), len(spec.entity_filters), len(spec.serializers)) == (1, 1, 1)
        assert (len(clone.identifiers), len(clone.entity_filters), len(clone.serializers)) == (2, 2, 2)

        assert spec.get_identifier(int) is None
        assert spec.get_entity_filter(int) is None
        assert len(spec.find_serializers(int)) == 0
        assert spec.include_fk is False


class TestIdentity:
    def test_pk(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c1=1, c2=None, c3=None)

        spec = ConfigurableSpec.create()
        ident = spec.get_identifier(m)

        assert ident and ident(v) == (1,)

    def test_not_set(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c2=2, c3=None)

        spec = ConfigurableSpec.create()
        ident = spec.get_identifier(m)

        assert ident and ident(v) is None

    def test_pks(self):
        m = define_model(table2, [GraphEntityMixin])
        v = m(c1=1, c2=2, c3=None)

        spec = ConfigurableSpec.create()
        ident = spec.get_identifier(m)

        assert ident and ident(v) == (1, 2)

    def test_no_pk(self):
        m = define_model(table3, [GraphEntityMixin])
        v = m(c1=1, c2=2, c3=None)

        spec = ConfigurableSpec.create()
        ident = spec.get_identifier(m)

        assert ident and ident(v) is None


class TestNull:
    def test_all_none(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c1=None, c2=None, c3=None)

        spec = ConfigurableSpec.create()
        ef = spec.get_entity_filter(m)

        assert ef and ef(v) is False

    def test_partial_none(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c1=1, c2=None, c3=None)

        spec = ConfigurableSpec.create()
        ef = spec.get_entity_filter(m)

        assert ef and ef(v) is True

    def test_no_column(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m()

        spec = ConfigurableSpec.create()
        ef = spec.get_entity_filter(m)

        assert ef and ef(v) is False


class TestFk:
    def _context(self, model) -> NodeContext:
        t = GraphTemplate([
            ("a", type(model), None, None),
        ])
        # Graph level serialization settings are not required in test cases.
        # Just need a factory instance to create a context.
        cxt = SerializationContext({}, lambda x:[])
        factory = NodeContextFactory(cxt, [], {})
        return factory.begin(Node(t.a, model, None, 0), [])

    def test_excludes(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()
        cxt = self._context(v)

        assert chain_serializers(spec.find_serializers(m))(cxt) == {"c1": 1, "c3": 3}

    def test_includes(self):
        m = define_model(table1, [GraphEntityMixin])
        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()
        spec.include_fk = True
        cxt = self._context(v)

        assert chain_serializers(spec.find_serializers(m))(cxt) == {"c1": 1, "c2": 2, "c3": 3}


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
        m = define_model(table1, [GraphEntityMixin], model_type=T1)

        class Ex(TypedDict):
            c2: Annotated[int, "fk"]

        def ex(cxt) -> Ex:
            return Ex(c2 = cxt.value.c2)

        # By default, c1, c3 are included -> alter serializer excludes c3 and merge Ex which contains c2 -> c1, c2 are included as a result.
        spec = ConfigurableSpec.create()
        spec.add_serializer(m, S.alter(ex, excludes={"c3"}))

        s = chain_serializers(spec.find_serializers(m))

        rt = inspect.signature(s).return_annotation

        assert walk_schema(Typeable.resolve(rt, m, spec)) == {"c1": int, "c2": int}
        assert walk_schema(Typeable.resolve(rt, m, spec), True) == {"c1": (int, "c1 in t1"), "c2": (int, "fk")}
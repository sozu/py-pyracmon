import pytest
import inspect
from pyracmon.model import Table, Column, define_model
from pyracmon.model_graph import *
from pyracmon.graph.schema import walk_schema, Typeable
from pyracmon.graph.serialize import chain_serializers


table1 = Table("t1", [
    Column("c1", int, None, True, False, "seq", "c1 in t1"),
    Column("c2", int, None, False, True, None, "c2 in t1"),
    Column("c3", int, None, False, False, None, "c3 in t1"),
])


table2 = Table("t2", [
    Column("c1", int, None, True, False, "seq"),
    Column("c2", int, None, True, True, None),
    Column("c3", int, None, False, False, None),
])


table3 = Table("t3", [
    Column("c1", int, None, False, False, "seq"),
    Column("c2", int, None, False, True, None),
    Column("c3", int, None, False, False, None),
])


class TestConfigurableSpec:
    def test_create(self):
        spec = ConfigurableSpec.create()

        assert spec.get_identifier(GraphEntityMixin) is not None
        assert spec.get_entity_filter(GraphEntityMixin) is not None
        assert len(spec.find_serializers(GraphEntityMixin)) == 1

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
    def test_excludes(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()

        assert chain_serializers(spec.find_serializers(type(v)))(None, None, None, v) == {"c1": 1, "c3": 3}

    def test_includes(self):
        m = define_model(table1, [GraphEntityMixin])

        v = m(c1=1, c2=2, c3=3)

        spec = ConfigurableSpec.create()
        spec.include_fk = True

        assert chain_serializers(spec.find_serializers(type(v)))(None, None, None, v) == {"c1": 1, "c2": 2, "c3": 3}


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
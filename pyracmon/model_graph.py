"""
This module provides graph specifications to deal with model types.

Most of them are not used directly except for `ConfigurableSpec` which is a configurable attribute of `PyracmonConfiguration` .
"""
from typing import *
from itertools import takewhile
import inspect
from .util import Configurable
from .graph.spec import GraphSpec
from .graph.schema import TypedDict, DynamicType, Shrink, issubgeneric, document_type
from .graph.serialize import T
from .graph.util import chain_serializers


class GraphEntityMixin:
    """
    Mixin class for model types which enables identity calculation and nullity check.
    """
    @classmethod
    def identity(cls, model: 'Model') -> Optional[Any]:
        """
        Returns primary key values as the identity of a model.

        :param model: A model object.
        :returns: Primary key value(s). ``None`` if the model type does not have primary key(s).
        """
        pks = [c.name for c in cls.columns if c.pk]
        if len(pks) > 0 and all([hasattr(model, n) and getattr(model, n) is not None for n in pks]):
            return tuple(map(lambda n: getattr(model, n), pks))
        else:
            return None

    @classmethod
    def is_null(cls, model: 'Model') -> bool:
        """
        Checks the model is considered to be null.

        :param model: A model object.
        :returns: Whether all column values are ``None`` .
        """
        return all([getattr(model, c.name, None) is None for c in cls.columns])


class ModelSchema(DynamicType[T]):
    """
    Schema of model type `T` .
    """
    @classmethod
    def fix(cls, bound, arg):
        class Schema(TypedDict):
            pass
        setattr(Schema, '__annotations__', {c.name:document_type(c.ptype, c.comment) for c in bound.columns})
        return Schema


class ExcludeFK(Shrink[T]):
    """
    Schema converter which excludes foreign key columns from schema of model type `T` .
    """
    @classmethod
    def select(cls, bound, arg):
        return {c.name for c in arg.columns if c.fk}, None


class ConfigurableSpec(GraphSpec, Configurable):
    """
    Extension of `GraphSpec` prepared to integrate model types into graph specification.

    This class exposes additional configurable attributes which controls the graph operation for model types.
    In global configuration, `graph_spec` attribute is an instance of this class,
    thus changes on it changes graph operations on model types.

    .. note:: The implementation of this class is not stable, don't depends on it.

    :param bool include_fk: Determines whether including foreign key columns in the result of graph serialization.
    """
    @classmethod
    def create(cls):
        spec = cls()

        spec.add_identifier(GraphEntityMixin, lambda m: type(m).identity(m))
        spec.add_entity_filter(GraphEntityMixin, lambda m: m and not type(m).is_null(m))

        def serialize(model:T) -> ModelSchema[T]:
            return {c.name:v for c, v in model}
        spec.add_serializer(GraphEntityMixin, serialize)

        return spec

    def __init__(self, *args):
        super(ConfigurableSpec, self).__init__(*args)

        self.include_fk = False

    def clone(self):
        spec = ConfigurableSpec(
            self.identifiers.copy(),
            self.entity_filters.copy(),
            self.serializers.copy(),
        )
        spec.include_fk = self.include_fk
        return spec

    def replace(self, another):
        self.identifiers[:] = another.identifiers
        self.entity_filters[:] = another.entity_filters
        self.serializers[:] = another.serializers
        self.include_fk = another.include_fk

    def _model_serializer(self, bases):
        """
        Generate configured serializer for model type.

        Parameters
        ----------
        bases: [Model -> object]
            Serialization functions.
        """
        if not self.include_fk:
            def serialize(c, n, b, model:T) -> ExcludeFK[T]:
                d = {c.name:v for c, v in model if not c.fk}
                return b(type(model)(**d))

            pos = next(filter(lambda ib: issubgeneric(inspect.signature(ib[1]).return_annotation, ModelSchema), enumerate(bases)), None)

            return bases[0:pos[0]+1] + [serialize] + bases[pos[0]+1:] if pos else bases
        else:
            return bases

    def find_serializers(self, t):
        bases = super(ConfigurableSpec, self).find_serializers(t)

        if issubclass(t, GraphEntityMixin):
            return self._model_serializer(bases)
        else:
            return bases


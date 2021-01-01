from typing import TypeVar
import inspect
from .util import Configurable
from .graph.spec import GraphSpec
from .graph.schema import TypedDict, DynamicType, Shrink, issubgeneric, document_type
from .graph.serialize import T


class GraphEntityMixin:
    """
    Mixin class for model types which enables identity calculation and nullity check.
    """
    @classmethod
    def identity(cls, model):
        """
        Returns primary key values as the identity of a model.

        Parameters
        ----------
        model: Model
            A model object.

        Returns
        -------
        tuple
            Primary keys as the identity of the model. If the model type has no primary key, returns `None`.
        """
        pks = [c.name for c in cls.columns if c.pk]
        if len(pks) > 0 and all([hasattr(model, n) and getattr(model, n) is not None for n in pks]):
            return tuple(map(lambda n: getattr(model, n), pks))
        else:
            return None

    @classmethod
    def is_null(cls, model):
        """
        Checks the model is considered to be null.

        Parameters
        ----------
        model: Model
            A model object.

        Returns
        -------
        bool
            `True` if all column values set to the model are `None`, otherwise `False`.
        """
        return all([getattr(model, c.name, None) is None for c in cls.columns])


class ModelSchema(DynamicType[T]):
    @classmethod
    def fix(cls, bound, arg):
        class Schema(TypedDict):
            pass
        setattr(Schema, '__annotations__', {c.name:document_type(c.ptype, c.comment) for c in bound.columns})
        return Schema


class ExcludeFK(Shrink[T]):
    @classmethod
    def select(cls, bound, arg):
        return {c.name for c in arg.columns if c.fk}, None


class ConfigurableSpec(GraphSpec, Configurable):
    """
    Extension of `GraphSpec` prepared to integrate model types into graph specification.

    This class exposes additional configurable attributes which controls the graph operation for model types.
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

    def _model_serializer(self, base):
        """
        Generate configured serializer for model type.

        Parameters
        ----------
        base: Model -> object
            Serialization function.
        """
        if not self.include_fk:
            # Use return annotation of base serializer if exists.
            rt = inspect.signature(base).return_annotation

            if issubgeneric(rt, ModelSchema):
                rt = ExcludeFK[rt]

            def serialize(model:T) -> rt:
                d = {c.name:v for c, v in model if not c.fk}
                return base(type(model)(**d)) if base else d
            return serialize
        else:
            return base

    def get_serializer(self, t):
        base = super(ConfigurableSpec, self).get_serializer(t)

        if issubclass(t, GraphEntityMixin):
            return self._model_serializer(base)
        else:
            return base


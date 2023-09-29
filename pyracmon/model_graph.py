"""
This module provides graph specifications to deal with model types.

Most of them are not used directly except for `ConfigurableSpec` which is an attribute of `PyracmonConfiguration` .
"""
from typing import Optional, Any, cast
import inspect
from .model import Model, Meta
from .graph.spec import GraphSpec
from .graph.typing import DynamicType, Shrink, document_type
from .graph.serialize import T, Serializer
from .graph.typing import TypedDict, issubgeneric


class GraphEntityMixin(Meta):
    """
    Mixin class for model types which enables identity calculation and nullity check.
    """
    @classmethod
    def identity(cls, model: Model) -> Optional[Any]:
        """
        Returns primary key values as the identity of a model.

        Args:
            model: A model object.
        Returns:
            Primary key value(s). `None` if the model type does not have primary key(s).
        """
        pks = [c.name for c in cls.columns if c.pk]
        if len(pks) > 0 and all([hasattr(model, n) and getattr(model, n) is not None for n in pks]):
            return tuple(map(lambda n: getattr(model, n), pks))
        else:
            return None

    @classmethod
    def is_null(cls, model: Model) -> bool:
        """
        Checks the model is considered to be null.

        Args:
            model: A model object.
        Returns:
            Whether all column values are `None`.
        """
        return all([getattr(model, c.name, None) is None for c in cls.columns])


class ModelSchema(DynamicType[T]):
    """
    Schema of model type `T`.
    """
    @classmethod
    def fix(cls, bound, arg):
        class Schema(TypedDict):
            pass
        bound = cast(type[Meta], bound)
        setattr(Schema, '__annotations__', {c.name:document_type(c.ptype, c.comment) for c in bound.columns})
        return Schema


class ExcludeFK(Shrink[T]):
    """
    Schema converter which excludes foreign key columns from schema of model type `T` .
    """
    @classmethod
    def select(cls, bound, arg):
        arg = cast(type[Meta], arg)
        return {c.name for c in arg.columns if c.fk}, None


class ConfigurableSpec(GraphSpec):
    """
    Extension of `GraphSpec` prepared to integrate model types into graph specification.

    This class exposes additional configurable attributes which controls the graph operation for model types.
    In global configuration, `graph_spec` attribute is an instance of this class,
    thus changes on it changes graph operations on model types.

    .. warning::
        The implementation of this class is not stable, don't depends on it.
    """
    @classmethod
    def create(cls):
        spec = cls()

        spec.add_identifier(GraphEntityMixin, lambda m: type(m).identity(m))
        spec.add_entity_filter(GraphEntityMixin, lambda m: m and not type(m).is_null(m))

        def serialize(cxt) -> ModelSchema[T]:
            return cast(ModelSchema, {c.name:v for c, v in cxt.value})
        spec.add_serializer(GraphEntityMixin, serialize)

        return spec

    def __init__(self, *args):
        super(ConfigurableSpec, self).__init__(*args)

        #: A flag which determines whether including foreign key columns in the result of graph serialization.
        self.include_fk = False

    def __deepcopy__(self, memo):
        spec = ConfigurableSpec(
            self.identifiers.copy(),
            self.entity_filters.copy(),
            self.serializers.copy(),
        )
        spec.include_fk = self.include_fk
        return spec

    def _model_serializer(self, bases: list[Serializer]) -> list[Serializer]:
        """
        Generate configured serializer for model type.

        Args:
            bases: Serialization functions.
        Returns:
        """
        if not self.include_fk:
            def serialize(cxt) -> ExcludeFK[T]:
                fk = {c.name for c, _ in cxt.value if c.fk}
                values = cxt.serialize()
                return cast(ExcludeFK, {c:v for c, v in values.items() if not c in fk})

            pos = next(filter(lambda ib: issubgeneric(inspect.signature(ib[1]).return_annotation, ModelSchema), enumerate(bases)), None)

            return bases[0:pos[0]+1] + [serialize] + bases[pos[0]+1:] if pos else bases
        else:
            return bases

    def find_serializers(self, t) -> list[Serializer]:
        bases = super(ConfigurableSpec, self).find_serializers(t)

        if issubclass(t, GraphEntityMixin):
            return self._model_serializer(bases)
        else:
            return bases
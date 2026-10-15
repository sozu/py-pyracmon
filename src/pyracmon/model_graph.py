"""
This module provides graph specifications to deal with model types.

`GraphEntityMixin` is a mixin class used by default if you create model types with `declare_models` in the `pyracmon` root package.
The mixin and `ConfigurableSpec` together provide the following additional features for model types:

- Model objects are identified by their primary key values, if they exist.
- Model objects are considered to be null if all column values are `None`, and such objects are not appended to the graph.
- Foreign key columns are excluded from the result of graph serialization by default.

A default `ConfigurableSpec` instance is already assigned to `PyracmonConfiguration.graph_spec`,
so in most cases you only need to customize this instance for graph functionality such as serialization or schema generation.

>>> from pyracmon.config import default_config
>>> spec = default_config.graph_spec
>>> spec.add_serializer(...)

When you need to use different configurations for different situations, such as a private API and a public API,
one way is to create new instances and use them separately.

```python
# In private API, all columns except password are serialized.
prv_spec = default_config().derive().graph_spec
prv_spec.add_serializer(user, excludes=["password"])

prv_user = prv_spec.to_schema(UserGraph, user=S.head())
def prv_handler():
    graph = ...
    res = prv_user.serialize(graph)
    ...

# In public API, only id and name are serialized.
pub_spec = default_config().derive().graph_spec
pub_spec.add_serializer(user, includes=["id", "name"])

pub_user = pub_spec.to_schema(UserGraph, user=S.head())
def pub_handler():
    graph = ...
    res = pub_user.serialize(graph)
    ...
```

A context manager is also available to temporarily change the configuration.
"""
import inspect
from itertools import takewhile
from typing import Any, Callable, TypeVar, cast, override, TYPE_CHECKING
from .model import Meta, IModel
from .graph.spec import GraphSpec
from .graph.typing import DynamicType, Shrink, document_type
from .graph.serialize import Serializer, NodeContext
from .graph.typing import TypedDict, issubgeneric


_T = TypeVar('_T')


if TYPE_CHECKING:
    class GraphEntityMixin(IModel):
        """
        A mixin class for model types that enables identity calculation and nullity checking.
        """
        @classmethod
        def identity(cls, model: IModel) -> Any | None:
            """
            Returns the primary key values as the identity of a model.

            Args:
                model: A model object.
            Returns:
                The primary key value(s), or `None` if the model type has no primary key.
            """
            ...

        @classmethod
        def is_null(cls, model: IModel) -> bool:
            """
            Checks whether the model is considered to be null.

            Args:
                model: A model object.
            Returns:
                Whether all of the model's column values are `None`.
            """
            ...
else:
    class GraphEntityMixin:
        @classmethod
        def identity(cls, model: IModel) -> Any | None:
            pks = [c.name for c in cls.columns if c.pk]
            if len(pks) > 0 and all([hasattr(model, n) and getattr(model, n) is not None for n in pks]):
                return tuple(map(lambda n: getattr(model, n), pks))
            else:
                return None

        @classmethod
        def is_null(cls, model: IModel) -> bool:
            return model is not None and all([getattr(model, c.name, None) is None for c in cls.columns])


class ModelSchema[T](DynamicType[T]):
    """
    The schema of a model type.

    Args:
        T: The model type.
    """
    @classmethod
    def fix(cls, bound, arg):
        bound = cast(type[Meta], bound)
        annotations = {c.name:document_type(c.ptype, c.comment) for c in bound.columns}
        return TypedDict('Schema', annotations) # type: ignore


class ExcludeFK[T](Shrink[T]):
    """
    A schema converter that excludes foreign key columns from the schema of a model type.

    Args:
        T: The model type.
    """
    @classmethod
    def select(cls, bound, arg):
        arg = cast(type[Meta], arg)
        return {c.name for c in arg.columns if c.fk}, None


def _serialize_model(cxt: NodeContext) -> ModelSchema[_T]:
    return cast(ModelSchema, {c.name:v for c, v in cxt.value})


def _exclude_fk(cxt: NodeContext) -> ExcludeFK[_T]:
    fk = {c.name for c, _ in cxt.value if c.fk}
    values = cxt.serialize()
    return cast(ExcludeFK, {c:v for c, v in values.items() if not c in fk})


class ConfigurableSpec(GraphSpec):
    """
    An extension of `GraphSpec` designed to integrate model types into the graph specification.

    This class exposes additional configurable attributes that control how graph operations use model types.

    .. warning::
        The implementation of this class is not stable. Do not depend on it.
    """
    @classmethod
    def create(cls):
        spec = cls()

        spec.add_identifier(GraphEntityMixin, lambda m: type(m).identity(m))
        spec.add_entity_filter(GraphEntityMixin, lambda m: m and not type(m).is_null(m))
        spec.add_serializer(GraphEntityMixin, _serialize_model)

        return spec

    def __init__(self, *args):
        super(ConfigurableSpec, self).__init__(*args)

        #: A flag that determines whether foreign key columns are included in the result of graph serialization.
        self.include_fk = False

    def __deepcopy__(self, memo):
        spec = ConfigurableSpec(
            self.identifiers.copy(),
            self.entity_filters.copy(),
            self.serializers.copy(),
        )
        spec.include_fk = self.include_fk
        return spec

    def _use_exclude_fk(self, bases: list[Serializer]) -> list[Serializer]:
        """
        Inserts a serializer to exclude foreign key columns if `include_fk` is `False`.
        """
        # ExcludeFk serializer should be inserted after the first serializer which returns ModelSchema.
        before = list(takewhile(lambda b: not issubgeneric(inspect.signature(b).return_annotation, ModelSchema), bases))
        pos = len(before)

        return before + [bases[pos],  _exclude_fk] + list(bases[pos+1:]) if pos < len(bases) else before

    @override
    def find_serializers(self, t: type) -> list[Callable[[NodeContext], Any]]:
        # Overrides `find_serializers` to apply additional configuration.
        bases = super(ConfigurableSpec, self).find_serializers(t)

        if issubclass(t, GraphEntityMixin):
            if not self.include_fk:
                bases = self._use_exclude_fk(bases)
            return bases
        else:
            return bases
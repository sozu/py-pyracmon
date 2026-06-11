"""
This module provides a type that holds the objects controlling how a graph behaves.
"""
from typing import Callable, Any, Self
from .identify import IdentifyPolicy, HierarchicalPolicy, neverPolicy
from .template import GraphTemplate, PropDef
from .serialize import Serializer, SerializationContext, NodeSerializer
from .schema import GraphSchema
from .typing import issubtype
from .graph import GraphView


type Identifier = Callable[[Any], Any]
type EntityFilter = Callable[[Any], bool]
type TemplateProperty = (
    type
    | tuple[()]
    | tuple[type]
    | tuple[type, Identifier | None]
    | tuple[type, Identifier | None, EntityFilter | None]
)


class GraphSpec:
    """
    Represents the specification of a graph that controls various behaviors in the graph's lifecycle.

    Three kinds of functions are the core specifications: *identifier*, *entity filter*, and *serializer*.

    - *Identifier* is a function that returns the identification value for a graph entity. See `Graph` for details on how this works.
    - *Entity filter* is a predicate function that determines whether an entity should be appended to the graph.
    - *Serializer* is a function that converts an entity into a serializable format. See `pyracmon.graph.serialize` for details.

    Every function should be registered with a type using the `add_xxx` methods.
    The function is applied to a graph node whose entity type is the same as, or a subtype of, the associated type.
    When multiple functions are applicable, the last registered one is applied.
    """
    def __init__(
        self,
        identifiers: list[tuple[type, Identifier]] | None = None,
        entity_filters: list[tuple[type, EntityFilter]] | None = None,
        serializers: list[tuple[type, Serializer]] | None = None,
    ):
        #: A list of pairs of a type and an *identifier*.
        self.identifiers: list[tuple[type, Identifier]] = identifiers or []
        #: A list of pairs of a type and an *entity filter*.
        self.entity_filters: list[tuple[type, EntityFilter]] = entity_filters or []
        #: A list of pairs of a type and a *serializer*.
        self.serializers: list[tuple[type, Serializer]] = serializers or []

    def _get_inherited[T](self, holder: list[tuple[type, T]], t: type) -> T | None:
        if not isinstance(t, type):
            return None
        return next(map(lambda x:x[1], filter(lambda x:issubtype(t, x[0]), holder)), None)

    def get_identifier(self, t: type) -> Callable[[Any], Any] | None:
        """
        Returns the most appropriate identifier for a type.

        Args:
            t: The type of an entity.
        Returns:
            The identifier, if one exists.
        """
        return self._get_inherited(self.identifiers, t)

    def get_entity_filter(self, t: type) -> Callable[[Any], bool] | None:
        """
        Returns the most appropriate entity filter for a type.

        Args:
            t: The type of an entity.
        Returns:
            The entity filter, if one exists.
        """
        return self._get_inherited(self.entity_filters, t)

    def find_serializers(self, t: type) -> list[Serializer]:
        """
        Returns a list of serializers applicable to a type.

        Args:
            t: The type of an entity.
        Returns:
            The applicable serializers.
        """
        if not isinstance(t, type):
            return []
        return list(map(lambda x:x[1], filter(lambda x:issubtype(t, x[0]), self.serializers[::-1])))

    def add_identifier[T](self, c: type[T], f: Callable[[T], Any]) -> Self:
        """
        Registers an identifier for a type.

        Args:
            c: The type to associate with the identifier.
            f: The identifier function.
        Returns:
            This instance, allowing method calls to be chained.
        """
        self.identifiers[0:0] = [(c, f)]
        return self

    def add_entity_filter[T](self, c: type[T], f: Callable[[T], bool]) -> Self:
        """
        Registers an entity filter for a type.

        Args:
            c: The type to associate with the entity filter.
            f: The entity filter function.
        Returns:
            This instance, allowing method calls to be chained.
        """
        self.entity_filters[0:0] = [(c, f)]
        return self

    def add_serializer(self, c: type, f: Serializer | NodeSerializer) -> Self:
        """
        Registers a serializer for a type.

        Args:
            c: The type to associate with the serializer.
            f: The serializer function.
        Returns:
            This instance, allowing method calls to be chained.
        """
        if isinstance(f, NodeSerializer):
            f = f.serializer
        self.serializers[0:0] = [(c, f)]
        return self

    def _make_policy(self, t: type, f: IdentifyPolicy | Callable[[Any], Any] | None) -> IdentifyPolicy:
        f = f or self.get_identifier(t)

        if isinstance(f, IdentifyPolicy):
            return f
        elif callable(f):
            return HierarchicalPolicy(f)
        else:
            return neverPolicy()

    def _get_property_definition(self, definition: TemplateProperty | type | GraphTemplate) -> PropDef:
        if isinstance(definition, GraphTemplate):
            return definition, neverPolicy(), None
        elif isinstance(definition, type):
            return definition, self._make_policy(definition, None), self.get_entity_filter(definition)
        elif isinstance(definition, tuple):
            # python >= 3.10
            match definition:
                case (k, ident, ef):
                    kind = k; identifier = ident; entity_filter = ef
                case (k, ident):
                    kind = k; identifier = ident; entity_filter = None
                case (k,):
                    kind = k; identifier = None; entity_filter = None
                case ():
                    kind = object; identifier = None; entity_filter = None
                case _:
                    raise ValueError(f"Invalid value was found in keyword arguments of new_template().")
            return kind, self._make_policy(kind, identifier), entity_filter or self.get_entity_filter(kind)
        else:
            raise ValueError(f"Invalid value was found in keyword arguments of new_template().")

    def new_template(self, *bases: GraphTemplate, **properties: TemplateProperty | GraphTemplate) -> GraphTemplate:
        """
        Creates a graph template from definitions of its template properties.

        Each keyword argument corresponds to a template property, where the key is the property name and the value is its definition.

        A property definition can be a `type` object or a tuple of at most three values.
        The former is equivalent to a tuple that contains only the `type` object.
        The values in the tuple are interpreted as the following attributes, in order.

        - The kind of the property, which indicates the type of the entity.
        - The *identifier* of the property.
        - The *entity filter* of the property.

        When the identifier or entity filter is omitted, the most appropriate one registered in this `GraphSpec` is applied.

        ```python
        template = GraphSpec().new_template(
            a = int,
            b = (str, lambda x:x),
            c = (str, lambda x:x, lambda x:len(x)>5),
        )
        ```

        Args:
            bases: The base templates whose properties and relations are merged into the new template.
            properties: The definitions of the template's properties.
        Returns:
            The created graph template.
        """
        base = sum(bases, GraphTemplate([]))

        return base + GraphTemplate([(n, *self._get_property_definition(d)) for n, d in properties.items()])

    def to_dict(self, graph: GraphView, _params_: dict[str, dict[str, Any]] = {}, **settings: NodeSerializer) -> dict[str, Any]:
        """
        Serializes a graph into a `dict`.

        Only nodes whose names appear in the keys of `settings` are serialized into the result.
        Each `NodeSerializer` object can be built using the factory methods on `pyracmon.graph.serialize.S`.

        ```python
        GraphSpec().to_dict(
            graph,
            a = S.of(),
            b = S.name("B"),
        )
        ```

        Args:
            graph: A view of the graph.
            _params_: The parameters passed to `SerializationContext` and used by *serializer*s.
            settings: A `NodeSerializer` for each property to be serialized.
        Returns:
            The serialization result.
        """
        return SerializationContext(settings, self.find_serializers, _params_).execute(graph)

    def to_schema(self, template: GraphTemplate, **settings: NodeSerializer) -> GraphSchema:
        """
        Creates a `GraphSchema` representing the structure of the serialization result under the given settings.

        Args:
            template: The template of the graph.
            settings: A `NodeSerializer` for each property to be serialized.
        Returns:
            The schema of the serialization result.
        """
        return GraphSchema(self, template, **settings)
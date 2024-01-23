"""
This module provides a type which contains objects to control how graphs work.
"""
from typing import Callable, Any, Optional, TypeVar, Union
from typing_extensions import Self
from .identify import IdentifyPolicy, HierarchicalPolicy, neverPolicy
from .template import GraphTemplate
from .serialize import Serializer, SerializationContext, NodeSerializer
from .schema import GraphSchema
from .typing import issubtype
from .graph import GraphView


T = TypeVar('T')


TypeDef = Union[type, GraphTemplate, GraphTemplate.Property]
Identifier = Callable[[Any], Any]
EntityFilter = Callable[[Any], bool]
TemplateProperty = Union[
    type,
    tuple[()],
    tuple[type],
    tuple[type, Optional[Identifier]],
    tuple[type, Optional[Identifier], Optional[EntityFilter]],
]


class GraphSpec:
    """
    This class contains the specifications of graph which control various behaviors in the lifecycles of graphs.

    3 kinds of functions are the core of graph behaviors: *identifier*, *entity filter* and *serializer* .

    *Identifier* and *entity filter* are used when appending values into graph.
    *Identifier* is a function to get a value used for the identification of graph entity. See `Graph` to know how this works.
    *Entity fliter* is a function to determine whether the entity should be appended to a graph or not.
    If `False` is returned for an entity, it is just ignored.

    See `pyracmon.graph.serialize` to know the detail of *serializer*.

    Each of them is bound to a `type` on registration to this and it affects nodes whose property type conforms to the `type` .
    """
    def __init__(
        self,
        identifiers: Optional[list[tuple[type, Identifier]]] = None,
        entity_filters: Optional[list[tuple[type, EntityFilter]]] = None,
        serializers: Optional[list[tuple[type, Serializer]]] = None,
    ):
        #: A list of pairs of type and *identifier*.
        self.identifiers: list[tuple[type, Identifier]] = identifiers or []
        #: A list of pairs of type and *entity_filter*.
        self.entity_filters: list[tuple[type, EntityFilter]] = entity_filters or []
        #: A list of pairs of type and *serializer*.
        self.serializers: list[tuple[type, Serializer]] = serializers or []

    def _get_inherited(self, holder: list[tuple[type, T]], t: type) -> Optional[T]:
        if not isinstance(t, type):
            return None
        return next(map(lambda x:x[1], filter(lambda x:issubtype(t, x[0]), holder)), None)

    def get_identifier(self, t: type) -> Optional[Callable[[Any], Any]]:
        """
        Returns the most appropriate identifier for a type.
        
        Args:
            t: Type of an entity.
        Returns:
            Identifier if exists.
        """
        return self._get_inherited(self.identifiers, t)

    def get_entity_filter(self, t: type) -> Optional[Callable[[Any], bool]]:
        """
        Returns the most appropriate entity filter for a type.
        
        Args:
            t: Type of an entity.
        Returns:
            Entity filter if exists.
        """
        return self._get_inherited(self.entity_filters, t)

    def find_serializers(self, t: type) -> list[Serializer]:
        """
        Returns a list of serializers applicable to a type.
        
        Args:
            t: Type of an entity.
        Returns:
            Serializers found.
        """
        if not isinstance(t, type):
            return []
        return list(map(lambda x:x[1], filter(lambda x:issubtype(t, x[0]), self.serializers[::-1])))

    def add_identifier(self, c: type, f: Callable[[Any], Any]) -> Self:
        """
        Register an identifier with a type.

        Args:
            c: A type bound to the identifier.
            f: An identifier function.
        Returns:
            This instance.
        """
        self.identifiers[0:0] = [(c, f)]
        return self

    def add_entity_filter(self, c: type, f: Callable[[Any], bool]) -> Self:
        """
        Register an entity filter with a type.

        Args:
            c: A type bound to the identifier.
            f: An entity filter function.
        Returns:
            This instance.
        """
        self.entity_filters[0:0] = [(c, f)]
        return self

    def add_serializer(self, c: type, f: Union[Serializer, NodeSerializer]) -> Self:
        """
        Register a serializer with a type.

        Args:
            c: A type bound to the identifier.
            f: A serializer function.
        Returns:
            This instance.
        """
        if isinstance(f, NodeSerializer):
            f = f.serializer
        self.serializers[0:0] = [(c, f)]
        return self

    def _make_policy(self, t: type, f: Union[IdentifyPolicy, Callable[[Any], Any], None]) -> IdentifyPolicy:
        f = f or self.get_identifier(t)

        if isinstance(f, IdentifyPolicy):
            return f
        elif callable(f):
            return HierarchicalPolicy(f)
        else:
            return neverPolicy()

    def _get_property_definition(self, definition: Union[
        TemplateProperty,
        type,
        GraphTemplate,
    ]) -> tuple[TypeDef, IdentifyPolicy, Optional[EntityFilter]]:
        if isinstance(definition, GraphTemplate):
            return definition, neverPolicy(), None
        elif isinstance(definition, type):
            return definition, self._make_policy(definition, None), self.get_entity_filter(definition)
        elif isinstance(definition, tuple):
            # python < 3.10
            if len(definition) == 3:
                kind, identifier, entity_filter = definition
            elif len(definition) == 2:
                kind, identifier, entity_filter = definition + (None,)
            elif len(definition) == 1:
                kind, identifier, entity_filter = definition + (None, None)
            elif len(definition) == 0:
                kind, identifier, entity_filter = (object, None, None)
            else:
                raise ValueError(f"Invalid value was found in keyword arguments of new_template().")
            # python >= 3.10
            #match definition:
            #    case (k, ident, ef):
            #        kind = k; identifier = ident; entity_filter = ef
            #    case (k, ident):
            #        kind = k; identifier = ident; entity_filter = None
            #    case (k,):
            #        kind = k; identifier = None; entity_filter = None
            #    case ():
            #        kind = object; identifier = None; entity_filter = None
            #    case _:
            #        raise ValueError(f"Invalid value was found in keyword arguments of new_template().")
            return kind, self._make_policy(kind, identifier), entity_filter or self.get_entity_filter(kind)
        else:
            raise ValueError(f"Invalid value was found in keyword arguments of new_template().")

    def new_template(self, *bases: GraphTemplate, **properties: Union[TemplateProperty, type, GraphTemplate]) -> GraphTemplate:
        """
        Creates a graph template with definitions of template properties.

        Each keyword argument corresponds to a template property where the key is proprety name and value is property definition.

        Property definition can be a `type` object or a tuple of at most 3 values.
        The former is the equivalent to a tuple which contains the `type` object alone.
        Values in the tuple are interpreted into following attributes in order.

        - The kind of property which indicates a type of entity.
        - *Identifier* of the property.
        - *Entity filter* of the property. 

        Omitted values are completed with registered items in this object.

        ```python
        template = GraphSpac().new_template(
            a = int,
            b = (str, lambda x:x),
            c = (str, lambda x:x, lambda x:len(x)>5),
        )
        ```

        Args:
            bases: Base templates whose properties and relations are merged into new template.
            properties: Definitions of template properties.
        Returns:
            Created graph template.
        """
        base = sum(bases, GraphTemplate([]))

        return base + GraphTemplate([(n, *self._get_property_definition(d)) for n, d in properties.items()])

    def to_dict(self, graph: GraphView, _params_: dict[str, dict[str, Any]] = {}, **settings: NodeSerializer) -> dict[str, Any]:
        """
        Serialize a graph into a `dict` .

        Only nodes whose names appear in keys of `settings` are serialized into the result.
        Each `NodeSerializer` object can be built by factory methods on `pyracmon.graph.serialize.S`.

        ```python
        GraphSpec().to_dict(
            graph,
            a = S.of(),
            b = S.name("B"),
        )
        ```

        Args:
            graph: A view of the graph.
            _params_: Parameters passed to `SerializationContext` and used by *serializer*s.
            settings: `NodeSerializer` for each property.
        Returns:
            Serialization result.
        """
        return SerializationContext(settings, self.find_serializers, _params_).execute(graph)

    def to_schema(self, template: GraphTemplate, **settings: NodeSerializer) -> GraphSchema:
        """
        Creates `GraphSchema` representing the structure of serialization result under given settings.

        Args:
            template: Template of a graph.
            settings: `NodeSerializer` for each property.
        Returns:
            Schema of serialization result.
        """
        return GraphSchema(self, template, **settings)
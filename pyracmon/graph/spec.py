from typing import *
from .identify import IdentifyPolicy, HierarchicalPolicy, neverPolicy
from .template import GraphTemplate
from .serialize import S, SerializationContext, NodeSerializer
from .schema import GraphSchema
from .util import Serializer, TemplateProperty
from .typing import issubtype


class GraphSpec:
    """
    This class contains the specifications of graph which control various behaviors in the lifecycles of graphs.

    Each instance contains 3 kind of functions; *identifier*, *entity filter* and *serializer*.

    *Identifier* is a function to get a value used for the identification of graph entity. See `Graph` to know how this works.

    *Entity fliter* is a function to determine whether the entity should be appended to a graph or not.
    If `False` is returned for an entity, it is just ignored.

    *Serializer* is a function which converts an entity value into a serializable object,
    whose signature is one of signatures described in `S.each`.
    In serialization phase, registered *serializer* s are first applied and *serializer* in `NodeSerializer` follows.

    Any kind of function is bound to a type when added, which will work as a key to determine whether it should be applied to a node.
    """
    def __init__(
        self,
        identifiers: List[Tuple[type, Callable[[Any], Any]]] = None,
        entity_filters: List[Tuple[type, Callable[[Any], bool]]] = None,
        serializers: List[Tuple[type, Serializer]] = None,
    ):
        #: A list of pairs of type and *identifier*.
        self.identifiers = identifiers or []
        #: A list of pairs of type and *entity_filter*.
        self.entity_filters = entity_filters or []
        #: A list of pairs of type and *serializer*.
        self.serializers = serializers or []

    def _get_inherited(self, holder, t):
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

    def find_serializers(self, t: type) -> List[Serializer]:
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

    def add_identifier(self, c: type, f: Callable[[Any], Any]) -> 'GraphSpec':
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

    def add_entity_filter(self, c: type, f: Callable[[Any], bool]) -> 'GraphSpec':
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

    def add_serializer(self, c: type, f: Serializer) -> 'GraphSpec':
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

    def _make_policy(self, t, f):
        f = f or self.get_identifier(t)

        if isinstance(f, IdentifyPolicy):
            return f
        elif callable(f):
            return HierarchicalPolicy(f)
        else:
            return neverPolicy()

    def _get_property_definition(self, d):
        if d is None or isinstance(d, tuple):
            d = iter(d or ())
            kind = next(d, None)
            ident = self._make_policy(kind, next(d, None))
            ef = next(d, self.get_entity_filter(kind))
            return kind, ident, ef
        elif isinstance(d, type):
            return d, self._make_policy(d, None), self.get_entity_filter(d)
        elif isinstance(d, GraphTemplate):
            return d, neverPolicy(), None
        else:
            raise ValueError(f"Invalid value was found in keyword arguments of new_template().")

    def new_template(self, *bases: GraphTemplate, **properties: TemplateProperty) -> GraphTemplate:
        """
        Creates a graph template with given definitions for template properties.

        Each keyword argument corresponds to a template property where the key is proprety name and value is property definition.

        Property definition can be a `type` object or a tuple of at most 3 values.
        The former is the equivalent to a tuple which contains the `type` object alone.
        Values in the tuple are interpreted into following attributes in order.

        - The kind of property which indicates a type of entity.
        - *Identifier* of the property.
        - *Entity filter* of the property. 

        Omitted values are complented with registered items in this object.

        >>> template = GraphSpac().new_template(
        >>>     a = int,
        >>>     b = (str, lambda x:x),
        >>>     c = (str, lambda x:x, lambda x:len(x)>5),
        >>> )

        Args:
            bases: Base templates whose properties and relations are merged into new template.
            properties: Definitions of template properties.
        Returns:
            Created graph template.
        """
        base = sum(bases, GraphTemplate([]))

        return base + GraphTemplate([(n, *self._get_property_definition(d)) for n, d in properties.items()])

    def to_dict(self, graph: 'GraphView', _params_: Dict[str, Dict[str, Any]] = {}, **settings: NodeSerializer) -> Dict[str, Any]:
        """
        Generates a dictionary representing structured entity values of a graph.

        Only nodes whose names appear in keys of `settings` are serialized into the result.
        Each `NodeSerializer` object can be built by factory methods on `S`.

        >>> GraphSpec().to_dict(
        >>>     graph,
        >>>     a = S.of(),
        >>>     b = S.name("B"),
        >>> )

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
from itertools import zip_longest
from functools import reduce, partial
from pyracmon.graph.identify import IdentifyPolicy, HierarchicalPolicy, neverPolicy
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.serialize import S, SerializationContext, NodeSerializer
from pyracmon.graph.schema import GraphSchema
from pyracmon.util import Configurable


class GraphSpec:
    """
    This class contains the specifications of graph which control various behaviors in the lifecycles of graphs.

    Each instance contains 3 kind of functions; *identifier*, *entity filter* and *serializer*.

    *Identifier* is a function to get a value used for the identification of graph entity. See `Graph` to know how this works.

    *Entity fliter* is a function returning `bool` to determine whether the entity should be appended to a graph or not.
    If `False` is returned for an entity, it is just ignored.

    *Serializer* is a function which converts an entity value into a serializable object.
    This is used in `to_dict()` to put entities into returning `dict`.
    Registered *serializer* is first applied to an entity value and other serialization functions in `NodeSerializer` are applied in order.

    They are registered in the specification via `add_xxx()` methods of this class with its bound `type`.
    The first function whose bound `type` is the subclass of the `kind` of the template property is choosed and applied.

    Attributes
    ----------
    identifiers: [(type, T -> ID)]
        A list of pairs of type and *identifier*.
    entity_filters: [(type, T -> bool)]
        A list of pairs of type and *entity filter*.
    serializers: [(type, T -> U)]
        A list of pairs of type and *serializer*.
    """
    def __init__(self, identifiers=None, entity_filters=None, serializers=None):
        self.identifiers = identifiers or []
        self.entity_filters = entity_filters or []
        self.serializers = serializers or []

    def _get_inherited(self, holder, t):
        if not isinstance(t, type):
            return None
        return next(map(lambda x:x[1], filter(lambda x:issubclass(t, x[0]), holder)), None)

    def get_identifier(self, t):
        return self._get_inherited(self.identifiers, t)

    def get_entity_filter(self, t):
        return self._get_inherited(self.entity_filters, t)

    def find_serializers(self, t):
        if not isinstance(t, type):
            return []
        return list(map(lambda x:x[1], filter(lambda x:issubclass(t, x[0]), self.serializers[::-1])))

    def add_identifier(self, c, f):
        """
        Register an *identifier* with the bound type.

        Parameters
        ----------
        c: type
            A type bound for the *identifier*.
        f: T -> ID
            An *identifier* function.

        Returns
        -------
        GraphSpec
            This instance.
        """
        self.identifiers[0:0] = [(c, f)]
        return self

    def add_entity_filter(self, c, f):
        """
        Register an *entity filter* with the bound type.

        Parameters
        ----------
        c: type
            A type bound for the *entity filter*.
        f: T -> bool
            An *entity filter* function.

        Returns
        -------
        GraphSpec
            This instance.
        """
        self.entity_filters[0:0] = [(c, f)]
        return self

    def add_serializer(self, c, f):
        """
        Register a *serializer* with the bound type.

        Parameters
        ----------
        c: type
            A type bound for the *serializer*.
        f: T -> U
            An *serializer* function.

        Returns
        -------
        GraphSpec
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

    def new_template(self, *bases, **template):
        """
        Creates a graph template with given definitions for template properties.

        Each keyword argument corresponds to a template property where the key is proprety name and value is property definition.

        Property definition can be given as a single value or a tuple of at most 3 values.
        The values are interpreted to property attributes in the following order.

        - The kind of entity value which must be a `type` object.
        - *Identifier* of the property.
        - *Entity filter* of the property.

        Parameters
        ----------
        bases: [GraphTemplate]
            Base templates whose properties and relations are merged into new template.
        template: {str: (type | Tuple[type, T -> ID, T -> bool])}
            Definitions of template properties.

        Returns
        -------
        GraphTemplate
            Created graph template.
        """
        base = sum(bases, GraphTemplate([]))

        return base + GraphTemplate([(n, *self._get_property_definition(d)) for n, d in template.items()])

    def to_dict(self, graph, __params={}, **settings):
        """
        Generates a dictionary representing structured entity values of a graph.

        Only nodes whose names appear in keys of `settings` are contained int the result.

        `NodeSerializer` object of each value in `settings` keyword arguments can be built by builder methods of `S`.

        Parameters
        ----------
        graph: GraphView
            A view of the graph.
        __params: {str: {str: object}}
            Parameters keyed by node names.
        settings: {str: NodeSerializer}
            Mapping from property name to `NodeSerializer` s.

        Returns
        -------
        Dict[str, object]
            A dictionary representing the graph.
        """
        return SerializationContext(settings, self.find_serializers, __params).execute(graph)

    def to_schema(self, template, **settings):
        """
        Creates `GraphSchema` under this specifications.

        Parameters
        ----------
        template: GraphTemplate
            A template of serializing graph.
        settings: {str: NodeSerializer}
            Mapping from property name to `NodeSerializer` s.

        Returns
        -------
        GraphSchema
            An object having schema information of serialized dictionary.
        """
        return GraphSchema(self, template, **settings)
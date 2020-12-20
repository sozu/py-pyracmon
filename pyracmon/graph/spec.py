from itertools import zip_longest
from functools import reduce, partial
from pyracmon.graph.identify import IdentifyPolicy, HierarchicalPolicy, neverPolicy
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.serialize import S, SerializationContext, NodeSerializer


class GraphSpec:
    """
    This class contains the specifications of graph which determine various behaviors in the lifecycle of graphs.

    Behaviors controlled by this specification is:

    - Identification of node entity when it is appended to a graph.
    - Filtering of node entity which filters entities to append into a graph.
    - Serialization of node entity.

    Every attribute is in the form of a list of pairs composed of a type and a function.
    The type determines whether to apply the function to the entity value by its type, that is,
    the function is applied to the value only when its type is a sub-class of the type.
    If multiple items fulfill the condition, only the latest registered one is used.

    Attributes
    ----------
    identifiers: [(type, T -> ID)]
        A list of pairs of type and function which extracts identifying key value from the entity.
    entity_filters: [(type, T -> bool)]
        A list of pairs of type and function which determines whether to append the entity into the graph.
    serializers: [(type, T -> U)]
        A list of pairs of type and function which converts the entity into a serializable value.
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

    def get_serializer(self, t):
        return self._get_inherited(self.serializers, t)

    def add_identifier(self, c, f):
        """
        Register a function which extracts identifying key value from the entity.

        The identification is based on declared type in the definition of graph template, not on an actual type of the entity.

        Parameters
        ----------
        c: type
            Super type of the entity to apply the function.
        f: T -> ID
            A function which extracts identifying key value from the entity.
        """
        self.identifiers[0:0] = [(c, f)]
        return self

    def add_entity_filter(self, c, f):
        """
        Register a function which determines whether to append the entity into the graph.

        The filtering is based on declared type in the definition of graph template, not on an actual type of the entity.

        Parameters
        ----------
        c: type
            Super type of the entity to apply the function.
        f: T -> bool
            A function which determines whether to append the entity into the graph.
        """
        self.entity_filters[0:0] = [(c, f)]
        return self

    def add_serializer(self, c, f):
        """
        Register a function which converts the entity into a serializable value.

        Parameters
        ----------
        c: type
            Super type of the entity to apply the function.
        f: T -> U
            A function which converts the entity into a serializable value.
        """
        self.serializers[0:0] = [(c, f)]
        return self

    def make_policy(self, t, f):
        f = f or self.get_identifier(t)

        if isinstance(f, IdentifyPolicy):
            return f
        elif callable(f):
            return HierarchicalPolicy(f)
        else:
            return neverPolicy()

    def get_property_definition(self, d):
        if d is None or isinstance(d, tuple):
            d = iter(d or ())
            kind = next(d, None)
            ident = self.make_policy(kind, next(d, None))
            ef = next(d, self.get_entity_filter(kind))
            return kind, ident, ef
        elif isinstance(d, type):
            return d, self.make_policy(d, None), self.get_entity_filter(d)
        elif isinstance(d, GraphTemplate):
            return d, neverPolicy(), None
        else:
            raise ValueError(f"Invalid value was found in keyword arguments of new_template().")

    def new_template(self, *bases, **template):
        """
        Creates a graph template with given definitions for template properties.

        Each template property definition can be given as a tuple contains at most 3 values:

        - The type of entity value.
        - A function which extracts an identifying key value from the entity.
        - A function which determines whether to append the entity into the graph.

        The first item is used to get identifier and entity filter from the specification, 
        and they can be overrided by second and third items respectively.

        Every value can be omitted, thereby minimus definition is `None` or `()`.
        When just an item whose type is `type` is given, it is supposed to be a tuple containing just the first item.

        Parameters
        ----------
        bases: [GraphTemplate]
            Base templates whose properties and relations are merged into new template.
        template: {str: (type, T -> ID, T -> bool) | type | None}
            Definitions of template properties.

        Returns
        -------
        GraphTemplate
            Created graph template.
        """
        base = sum(bases, GraphTemplate([]))

        return base + GraphTemplate([(n, *self.get_property_definition(d)) for n, d in template.items()])

    def to_dict(self, graph, **settings):
        """
        Generates a dictionary representing structured values of a graph.

        Only nodes whose names appear in keys of `serializers` are used.

        Parameters
        ----------
        graph: Graph.View
            A view of the graph.
        serializers: {str: NodeSerializer | (str | str -> str, [T] -> T | int, T -> U)}
            Mapping from property name to `NodeSerializer` s or their equivalents.

        Returns
        -------
        Dict[str, object]
            A dictionary representing the graph.
        """
        return SerializationContext(settings, self.get_serializer).execute(graph)
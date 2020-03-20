from itertools import zip_longest
from pyracmon.graph.graph import IdentifyPolicy
from pyracmon.graph.template import P, GraphTemplate
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
    def __init__(self, identifiers = None, entity_filters = None, serializers = None):
        self.identifiers = identifiers or []
        self.entity_filters = entity_filters or []
        self.serializers = serializers or []

    def get_identifier(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.identifiers), (None, None))[1]

    def get_entity_filter(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.entity_filters), (None, None))[1]

    def get_serializer(self, v):
        t = v if isinstance(v, type) else type(v)
        return next(filter(lambda x: issubclass(t, x[0]), self.serializers), (None, None))[1]

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
        def make_identifier(f):
            if isinstance(f, IdentifyPolicy):
                return f
            elif callable(f):
                return IdentifyPolicy.hierarchical(f)
            else:
                return IdentifyPolicy.never()
        def definition(d):
            if d is None or d == ():
                return None, make_identifier(None), None
            elif isinstance(d, P):
                return d.kind, make_identifier(d.identifier), d.entity_filter
            elif isinstance(d, tuple):
                kind = d[0] if len(d) >= 1 else None
                ident = make_identifier(d[1] if len(d) >= 2 else self.get_identifier(kind))
                ef = d[2] if len(d) >= 3 else self.get_entity_filter(kind)
                return kind, ident, ef
            elif isinstance(d, type):
                return d, make_identifier(self.get_identifier(d)), self.get_entity_filter(d)
            else:
                raise ValueError(f"Invalid value was found in keyword arguments of new_template().")

        template = GraphTemplate([(n, *definition(d)) for n, d in template.items()])

        for t in bases:
            for p in t._properties:
                if hasattr(template, p.name):
                    raise ValueError(f"Template property '{p.name}' conflicts.")
                prop = GraphTemplate.Property(template, p.name, p.kind, p.identifier, p.entity_filter, origin=p)
                template._properties.append(prop)
                setattr(template, p.name, prop)
            for f, t in t._relations:
                getattr(template, f.name) >> getattr(template, t.name)

        return template

    def to_dict(self, graph, **serializers):
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
        def to_serializer(s):
            if isinstance(s, NodeSerializer):
                return s
            else:
                settings = [(p[0] or p[1]) for p in zip_longest(s, (None, None, None), fillvalue=None)]
                return S.of(*settings)

        context = SerializationContext(dict([(n, to_serializer(s)) for n, s in serializers.items()]), self.get_serializer)

        result = {}

        for c in filter(lambda c: c().property.parent is None, graph):
            context.serialize_to(c().name, c, result)

        return result
from itertools import zip_longest
from pyracmon.graph.graph import IdentifyPolicy
from pyracmon.graph.template import P, GraphTemplate
from pyracmon.graph.serialize import S, SerializationContext, NodeSerializer


class GraphSpec:
    def __init__(self, identifiers = None, entity_filters = None, serializers = None):
        self.identifiers = identifiers or []
        self.entity_filters = entity_filters or []
        self.serializers = serializers or []

    def get_identifier(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.identifiers), (None, None))[1]

    def get_entity_filter(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.entity_filters), (None, None))[1]

    def get_serializer(self, v):
        return next(filter(lambda x: isinstance(v, x[0]), self.serializers), (None, None))[1]

    def add_identifier(self, c, f):
        self.identifiers[0:0] = [(c, f)]

    def add_entity_filter(self, c, f):
        self.entity_filters[0:0] = [(c, f)]

    def add_serializer(self, c, f):
        self.serializers[0:0] = [(c, f)]

    def new_template(self, **template):
        """
        Creates template of a graph.

        Parameters
        ----------
        template: {str: type | (type, T -> object)}
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
        return GraphTemplate([(n, *definition(d)) for n, d in template.items()])

    def to_dict(self, __graph__, **serializers):
        """
        Generates a dictionary representing structured values of a graph.

        Only nodes whose names appear in keys of `serializers` are stored in returned dictionary.

        Parameters
        ----------
        __graph__: Graph.View
            A view of the graph.
        serializers: {str: (str -> str, [T] -> T, U -> object)}
            A dictionary where the key specifies a node name and the value is a serialization settings for the node.

        Returns
        -------
        {str: object}
            A dictionary representing the graph.
        """
        noop = lambda x: x
        def serialize(x):
            f = self.get_serializer(x)
            return f(x) if f else x

        def to_serializer(s):
            if isinstance(s, NodeSerializer):
                if s.serializer is None:
                    s.serializer = serialize
                return s
            else:
                settings = [(p[0] or p[1]) for p in zip_longest(s, (None, noop, serialize), fillvalue=None)]
                return S.of(*settings)

        context = SerializationContext(dict([(n, to_serializer(s)) for n, s in serializers.items()]))

        result = {}

        for c in filter(lambda c: c().property.parent is None, __graph__):
            context.serialize_to(c().name, c, result)

        return result
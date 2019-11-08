from itertools import zip_longest
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.serialize import SerializationContext, NodesSerializer


class GraphSpec:
    def __init__(self, identifiers = None, serializers = None):
        self.identifiers = identifiers or []
        self.serializers = serializers or []

    def get_identifier(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.identifiers), (None, None))[1]

    def get_serializer(self, v):
        return next(filter(lambda x: isinstance(v, x[0]), self.serializers), (None, None))[1]

    def add_identifier(self, c, f):
        self.identifiers.append((c, f))

    def add_serializer(self, c, f):
        self.serializers.append((c, f))

    def new_template(self, **template):
        """
        Creates template of a graph.

        Parameters
        ----------
        template: {str: type | (type, T -> object)}
        """
        def definition(d):
            if d is None or d == ():
                return None, None
            elif isinstance(d, tuple):
                kind = d[0] if len(d) >= 1 else None
                ident = d[1] if len(d) >= 2 else self.get_identifier(kind)
                return kind, ident
            elif isinstance(d, type):
                return d, self.get_identifier(d)
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
            settings = [(p[0] or p[1]) for p in zip_longest(s, (noop, noop, serialize), fillvalue=None)]
            return NodesSerializer(settings[0], settings[1], settings[2])
        context = SerializationContext(dict([(n, to_serializer(s)) for n, s in serializers.items()]))

        result = {}

        for c in filter(lambda c: c().property.parent is None, __graph__):
            kv = context.serialize(c().name, c)
            if kv:
                result[kv[0]] = kv[1]

        return result
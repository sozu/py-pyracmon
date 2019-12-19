from itertools import zip_longest
from pyracmon.graph.graph import IdentifyPolicy
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.serialize import SerializationContext, NodeSerializer, _expand


class GraphSpec:
    def __init__(self, identifiers = None, serializers = None):
        self.identifiers = identifiers or []
        self.serializers = serializers or []

    def get_identifier(self, t):
        return next(filter(lambda x: issubclass(t, x[0]), self.identifiers), (None, None))[1]

    def get_serializer(self, v):
        return next(filter(lambda x: isinstance(v, x[0]), self.serializers), (None, None))[1]

    def add_identifier(self, c, f):
        self.identifiers[0:0] = [(c, f)]

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
                return None, make_identifier(None)
            elif isinstance(d, tuple):
                kind = d[0] if len(d) >= 1 else None
                ident = make_identifier(d[1] if len(d) >= 2 else self.get_identifier(kind))
                return kind, ident
            elif isinstance(d, type):
                return d, make_identifier(self.get_identifier(d))
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
            settings = [(p[0] or p[1]) for p in zip_longest(s, (None, noop, serialize), fillvalue=None)]
            return NodeSerializer(settings[0], settings[1], settings[2])
        context = SerializationContext(dict([(n, to_serializer(s)) for n, s in serializers.items()]))

        result = {}

        for c in filter(lambda c: c().property.parent is None, __graph__):
            #kv = context.serialize(c().name, c)
            nv = context.serialize(c().name, c)
            if nv:
                namer, value = nv
                if namer is None:
                    result[c().name] = value
                elif isinstance(namer, str):
                    result[namer] = value
                elif callable(namer) and isinstance(value, dict):
                    value = _expand(value)
                    if value:
                        result.update(dict([(namer(k), v) for k, v in value.items()]))

        return result
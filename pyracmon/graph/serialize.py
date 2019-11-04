from itertools import zip_longest


def graph_dict(graph, **serializers):
    """
    Generates a dictionary representing structured values of a graph.

    Only nodes whose names appear in keys of `serializers` are stored in returned dictionary.

    Parameters
    ----------
    graph: Graph.View
        A view of the graph.
    serializers: {str: (str -> str, [T] -> T, U -> object)}
        A dictionary where the key specifies a node name and the value is a serialization settings for the node.

    Returns
    -------
    {str: object}
        A dictionary representing the graph.
    """
    noop = lambda x: x

    def to_serializer(s):
        settings = [(p[0] or p[1]) for p in zip_longest(s, (noop, noop, noop), fillvalue=None)]
        return NodesSerializer(settings[0], settings[1], settings[2])
    context = SerializationContext(dict([(n, to_serializer(s)) for n, s in serializers.items()]))

    result = {}

    for c in filter(lambda c: c().property.parent is None, graph):
        kv = context.serialize(c().name, c)
        if kv:
            result[kv[0]] = kv[1]

    return result


def head(vs):
    return vs[0] if len(vs) > 0 else None


class NodesSerializer:
    def __init__(self, namer, aggregator, serializer):
        self.namer = namer
        self.aggregator = aggregator
        self.serializer = serializer


class SerializationContext:
    def __init__(self, serializers):
        self.serializers = serializers

    def serialize(self, name, nodes):
        s = self.serializers.get(name, None)
        return (s.namer(name), s.aggregator([self.serialize_nodes(s.serializer, n) for n in nodes])) if s else None

    def serialize_nodes(self, serializer, node):
        entity = serializer(node())

        if isinstance(entity, dict):
            for n, c in node:
                kv = self.serialize(n, c)
                if kv:
                    entity[kv[0]] = kv[1]

        return entity

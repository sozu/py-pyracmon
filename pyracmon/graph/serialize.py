def head(vs):
    return vs[0] if len(vs) > 0 else None


class NodeSerializer:
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


class FixedNamer:
    @property
    def name(self):
        raise NotImplementedError()


class FixedAggregator:
    def aggregate(self, original_type):
        raise NotImplementedError()


class FixedSerializer:
    def serialize(self, original_type):
        raise NotImplementedError()
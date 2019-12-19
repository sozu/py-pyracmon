def as_is(x):
    return x

def head(vs):
    return vs[0] if len(vs) > 0 else None


class NodeSerializer:
    def __init__(self, namer, aggregator, serializer):
        self.namer = namer
        self.aggregator = aggregator
        self.serializer = serializer


def _expand(v):
    if isinstance(v, dict):
        return v
    elif isinstance(v, (list, tuple)) and len(v) > 0:
        return v[0] if isinstance(v[0], dict) else None
    else:
        return None


class SerializationContext:
    def __init__(self, serializers):
        self.serializers = serializers

    def serialize(self, name, nodes):
        s = self.serializers.get(name, None)
        return (s.namer, s.aggregator([self.serialize_nodes(s.serializer, n) for n in nodes])) if s else None

    def serialize_nodes(self, serializer, node):
        entity = serializer(node())

        if isinstance(entity, dict):
            for n, c in node:
                nv = self.serialize(n, c)
                if nv:
                    namer, value = nv
                    if namer is None:
                        entity[n] = value
                    elif isinstance(namer, str):
                        entity[namer] = value
                    elif callable(namer):
                        value = _expand(value)
                        if value:
                            entity.update(dict([(namer(k), v) for k, v in value.items()]))

        return entity
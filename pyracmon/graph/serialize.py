def as_is(x):
    return x

def head(vs):
    return vs[0] if len(vs) > 0 else None


class S:
    @classmethod
    def of(cls, namer=None, aggregator=None, serializer=None):
        if namer and not isinstance(namer, str) and not callable(namer):
            raise ValueError(f"Naming element must be a string or callable object but {type(namer)} is given.")
        if aggregator and not isinstance(aggregator, int) and not callable(aggregator):
            raise ValueError(f"Aggregation element must be an integer or callable object but {type(aggregator)} is given.")
        if serializer and not callable(serializer):
            raise ValueError(f"Serialization element must be a callable object but {type(serializer)} is given.")

        return NodeSerializer(namer, aggregator, serializer)


class NodeSerializer:
    def __init__(self, namer, aggregator, serializer):
        self.namer = namer
        self.aggregator = aggregator
        self.serializer = serializer

    def name_of(self, name):
        return name if not self.namer else \
            self.namer if isinstance(self.namer, str) else \
                self.namer(name)

    def aggregation_of(self, values):
        if self.aggregator is None:
            return values
        elif isinstance(self.aggregator, int):
            return values[self.aggregator] if len(values) > self.aggregator else None
        else:
            return self.aggregator(values)

    def serialization_of(self, finder, value):
        base = finder(value)
        return self.serializer(base or as_is, value) if self.serializer \
            else base(value) if base else value

    @property
    def be_merged(self):
        return callable(self.namer)

    @property
    def be_singular(self):
        # TODO a serializer is considered to aggregate values into a value when the aggregator is set currently.
        return self.aggregator is not None

    def name(self, namer):
        self.namer = namer
        return self

    def head(self):
        self.aggregator = lambda vs: vs[0] if len(vs) > 0 else None
        return self

    def tail(self):
        self.aggregator = lambda vs: vs[-1] if len(vs) > 0 else None
        return self

    def fold(self, aggregator):
        self.aggregator = aggregator
        return self

    def each(self, func):
        self.serializer = func
        return self


def _expand(v):
    if isinstance(v, dict):
        return v
    elif isinstance(v, (list, tuple)) and len(v) > 0:
        return v[0] if isinstance(v[0], dict) else {}
    else:
        return {}


class SerializationContext:
    def __init__(self, serializers, serializer_finder):
        self.serializers = serializers
        self.serializer_finder = serializer_finder

    def serialize_to(self, name, nodes, parent):
        """
        Append values converted from nodes to the parent dictionary.
        """
        s = self.serializers.get(name, None)

        if s:
            value = s.aggregation_of([self._serialize_node(s, n) for n in nodes])

            if s.be_merged:
                parent.update({s.name_of(k): v for k, v in _expand(value).items()})
            else:
                parent[s.name_of(name)] = value

    def _serialize_node(self, serializer, node):
        value = serializer.serialization_of(self.serializer_finder, node())

        if isinstance(value, dict):
            for n, c in node:
                self.serialize_to(n, c, value)

        return value
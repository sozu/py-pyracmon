from functools import partial


def as_is(x):
    return x

def head(vs, alt=None):
    return vs[0] if len(vs) > 0 else alt


class S:
    """
    An utility class to construct `NodeSerializer`.
    """
    @classmethod
    def of(cls, namer=None, aggregator=None, serializer=None):
        """
        Create an instance of `NodeSerializer` with arguments.

        Parameters
        ----------
        namer: str | str -> str
            A key string or naming function. `None` specifies that the property name is used as it is.
        aggregator: [T] -> T | int
            An aggregation function or an index at which the value is selected from converted values in aggregation phase.
        serializer: T -> U
            An function converting node entity into a serializable value. If `None`, the first matching serializer in `GraphSpec` is used.

        Returns
        -------
        NodeSerializer
            Created instance.
        """
        if namer and not isinstance(namer, str) and not callable(namer):
            raise ValueError(f"Naming element must be a string or callable object but {type(namer)} is given.")
        if aggregator and not isinstance(aggregator, int) and not callable(aggregator):
            raise ValueError(f"Aggregation element must be an integer or callable object but {type(aggregator)} is given.")
        if serializer and not callable(serializer):
            raise ValueError(f"Serialization element must be a callable object but {type(serializer)} is given.")

        return NodeSerializer(namer, aggregator, serializer)


class NodeSerializer:
    """
    The instance of this class contains the configurations to convert a `NodeContainer` into a serializable value.

    This class is designed to be used in the keyword argument of `GraphSpec.to_dict()` with the name of corresponding node property name.
    For simplicity, there are two ways to create the instance, with tuple and with `S`.

    >>> template = spec.new_template(a = dict, b = dict)
    >>> graph = new_graph(template)
    >>> graph.append(a = dict(a1=1, a2=2), b = dict(b1=3, b2=4))
    >>> spec.to_dict(graph, a = S.of(), b = ())
    {"a":[{"a1":1, "a2":2, "b":[{"b1":3, "b2":4}]}]}

    The conversion is composed to 3 phases.

    1. Convert the entity of each node in the container into a serializable value with `serializer` function.
    2. If `aggregator` is set, aggregate the list of converted values into a value with it.
    3. Put the converted value(s) into the dictionary which is the converted result of parent node with a key determined by `namer`.

    Using callable as `namer` causes a tricky behavior which merges values into parent dictionary.

    >>> graph.append(a = dict(a1=1, a2=2), b = dict(b1=3, b2=4))
    >>> spec.to_dict(graph, a = S.of(), b = (lambda x: f"__x__",))
    {"a": [{"a1":1, "a2":2, "__b1__":3, "__b2__":4}]}

    In this case, the property name `b` is ignored and converted values are forcibly aggregated even when no `aggregator` is set.
    Also, (each) converted value must be a `dict`, otherwise, all values are discarded.
    """
    def __init__(self, namer, aggregator, serializer):
        self.namer = namer
        self.aggregator = aggregator
        self.serializer = serializer

    def _name_of(self, name):
        return name if not self.namer else \
            self.namer if isinstance(self.namer, str) else \
                self.namer(name)

    def _aggregation_of(self, values):
        if self.aggregator is None:
            return values
        elif isinstance(self.aggregator, int):
            return values[self.aggregator] if len(values) > self.aggregator else None
        else:
            return self.aggregator(values)

    def _serialization_of(self, finder, value):
        base = finder(value)
        if self.serializer:
            if base:
                return self.serializer(partial(base, as_is), value)
            else:
                return self.serializer(as_is, value)
        elif base:
            return base(as_is, value)
        else:
            return value

    @property
    def be_merged(self):
        return callable(self.namer)

    @property
    def be_singular(self):
        # TODO a serializer is considered to aggregate values into a value when the aggregator is set currently.
        return self.aggregator is not None

    def name(self, namer):
        """
        Set a key in parent dictionary.

        TODO Deny the argument when it is not `str`.

        Parameters
        ----------
        namer: str
            A key string.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.namer = namer
        return self

    def merge(self, namer=None):
        """
        Set a naming function taking a property name and returning a key in parent dictionary.

        TODO Deny the argument when ist is not callable.

        By using callable as `namer`, key-value pairs of the converted result are merged into parent dictionary.

        Parameters
        ----------
        namer: str -> str
            The naming function. If `None`, the property name is returned as it is.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.namer = namer or (lambda x:x)
        return self

    def head(self, alt=None):
        """
        Set an aggregator which picks up the first element.

        Parameters
        ----------
        alt: object
            A value used when there exists no nodes.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.aggregator = lambda vs: vs[0] if len(vs) > 0 else alt
        return self

    def tail(self, alt=None):
        """
        Set an aggregator which picks up the last element.

        Parameters
        ----------
        alt: object
            A value used when there exists no nodes.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.aggregator = lambda vs: vs[-1] if len(vs) > 0 else alt
        return self

    def fold(self, aggregator):
        """
        Set an aggregation function converting a list of values into a serializable value.

        Parameters
        ----------
        aggregator: [T] -> T
            An aggregation function.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.aggregator = aggregator
        return self

    def each(self, func):
        """
        Set a function converting a node entity into a serializable value.

        To collect child nodes into the result of `to_dict()`, serialization function MUST returns a `dict`.

        The function should takes 2 arguments, where the first one is default serializer and the second one is a target value.
        The default serializer is one of followings.

        - The last registered converting function if any.
        - Serializer registered in `GraphSpec` for the value type if any.
        - Identity function which returns the argument as it is.

        Parameters
        ----------
        func: (S -> T), T -> U
            A function converting a node entity (`T`) into a serializable value (`U`).

        Returns
        -------
        NodeSerializer
            This instance.
        """
        if not self.serializer:
            self.serializer = func
        else:
            old = self.serializer
            def then(s, v):
                return func(partial(old, s), v)
            self.serializer = then
        return self


def _expand(v):
    if isinstance(v, dict):
        return v
    elif isinstance(v, (list, tuple)) and len(v) > 0:
        return v[0] if isinstance(v[0], dict) else {}
    else:
        return {}


class SerializationContext:
    """
    This class contains a set of `NodeSerializer`s and the interface to find serialization function registered in `GraphSpec`.
    """
    def __init__(self, serializers, serializer_finder):
        self.serializers = serializers
        self.serializer_finder = serializer_finder

    def serialize_to(self, name, nodes, parent):
        """
        Convert nodes into serializable values and append them into the parent dictionary.

        Parameters
        ----------
        name: str
            A property name for the nodes.
        nodes: [Node]
            A list of nodes. 
        parent: dict
            A parent dictionary the converted values are appended to.
        """
        s = self.serializers.get(name, None)

        if s:
            value = s._aggregation_of([self._serialize_node(s, n) for n in nodes])

            if s.be_merged:
                parent.update({s._name_of(k): v for k, v in _expand(value).items()})
            else:
                parent[s._name_of(name)] = value

    def _serialize_node(self, serializer, node):
        value = serializer._serialization_of(self.serializer_finder, node())

        if isinstance(value, dict):
            for n, c in node:
                self.serialize_to(n, c, value)

        return value
from functools import partial, reduce


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
        s = NodeSerializer()
        if namer:
            s._set_namer(namer)
        if aggregator:
            s._set_aggregator(aggregator)
        if serializer:
            s.each(serializer)

        return s

    @classmethod
    def factory(cls, factory):
        def f(*args, **kwargs):
            return factory(S.of(), *args, **kwargs)
        setattr(S, factory.__name__, f)
        return factory


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
    def __init__(self):
        self.namer = None
        self.aggregator = None
        self._serializers = []

    def _name_of(self, name):
        return name if not self.namer else \
            self.namer if isinstance(self.namer, str) else \
                self.namer(name)

    def _set_namer(self, namer):
        if isinstance(namer, str):
            return self.name(namer)
        elif callable(namer):
            return self.merge(namer)
        else:
            raise ValueError(f"Naming element must be a string or callable object but {type(namer)} is given.")

    def _set_aggregator(self, aggregator):
        if isinstance(aggregator, int):
            return self.at(aggregator)
        elif callable(aggregator):
            return self.fold(aggregator)
        else:
            raise ValueError(f"Aggregation element must be an integer or callable object but {type(aggregator)} is given.")

    def _aggregation_of(self, values):
        if self.aggregator is None:
            return values
        elif isinstance(self.aggregator, int):
            return values[self.aggregator] if len(values) > self.aggregator else None
        else:
            return self.aggregator(values)

    def _serialization_of(self, finder, value):
        base = finder(value)
        s = reduce(lambda acc,x: partial(x, acc), ([base] if base else []) + self._serializers, as_is)
        return s(value)

    @property
    def be_merged(self):
        return callable(self.namer)

    @property
    def be_singular(self):
        return self.aggregator is not None

    @S.factory
    def name(self, name):
        """
        Set a key in parent dictionary.

        Parameters
        ----------
        name: str
            A key string.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        if not isinstance(name, str):
            raise ValueError(f"The name of node must be a string but {type(name)} is given.")
        self.namer = name
        return self

    @S.factory
    def merge(self, namer=None):
        """
        Set a naming function taking a property name and returning a key in parent dictionary.

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
        if namer and not callable(namer):
            raise ValueError(f"The method merging a node into its parent node must be callable or None.")
        self.namer = namer or (lambda x:x)
        return self

    @S.factory
    def at(self, index, alt=None):
        """
        Set an aggregator which picks up the element at the index.

        Parameters
        ----------
        index: int
            An index of the element.
        alt: object
            A value used when no node is found at the index.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self.aggregator = lambda vs: vs[index] if len(vs) > index else alt
        return self

    @S.factory
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

    @S.factory
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

    @S.factory
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

    @S.factory
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
        self._serializers.append(func)
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
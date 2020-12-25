from functools import partial, reduce
from inspect import signature, Signature
from typing import TypeVar
from .template import GraphTemplate
from .graph import Node


T = TypeVar('T')


class S:
    """
    An utility class to construct `NodeSerializer`.
    """
    @classmethod
    def of(cls, namer=None, aggregator=None, *serializers):
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
        return NodeSerializer(namer, aggregator, *serializers)

    @classmethod
    def builder(cls, builder):
        def f(*args, **kwargs):
            return builder(S.of(), *args, **kwargs)
        setattr(S, builder.__name__, f)
        return builder


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
    def __init__(self, namer=None, aggregator=None, *serializers):
        self._namer = namer
        self._aggregator = aggregator
        self._serializers = list(serializers)

    @property
    def namer(self):
        if not self._namer:
            return lambda n: n
        elif isinstance(self._namer, str):
            return lambda n: self._namer
        else:
            return lambda n: self._namer(n)

    @property
    def aggregator(self):
        if self._aggregator is None:
            def agg(values: [T]) -> [T]:
                return values
            return agg
        elif signature(self._aggregator).return_annotation == Signature.empty:
            def agg(values: [T]) -> [T]:
                return self._aggregator(values)
            return agg
        else:
            return self._aggregator

    @property
    def serializer(self):
        def wrap(f):
            try:
                sig = signature(f)
                def g(cxt, node, base, value) -> sig.return_annotation:
                    ba = sig.bind(*(value, base, node, cxt)[len(sig.parameters)-1::-1])
                    return f(*ba.args)
                return g
            except:
                def g(cxt, node, base, value):
                    return f(value)
                return g

        funcs = [wrap(s) for s in self._serializers]
        rt = next(filter(lambda rt: rt != Signature.empty, map(lambda f: signature(f).return_annotation, funcs[::-1])), Signature.empty)

        def composed(cxt, node, base, value) -> rt:
            return reduce(lambda acc,f: partial(f, cxt, node, acc), [base or as_is] + funcs)(value)

        return composed

    @property
    def be_merged(self):
        return callable(self._namer)

    @property
    def be_singular(self):
        return not isinstance(signature(self.aggregator).return_annotation, list)

    def _set_aggregator(self, aggregator, folds):
        try:
            rt = signature(aggregator).return_annotation
        except:
            rt = Signature.empty

        if rt == Signature.empty:
            def agg(vs: [T]) -> (T if folds else [T]):
                return aggregator(vs)
            self._aggregator = agg
        elif isinstance(rt, list) ^ (not folds):
            raise ValueError(f"Return annotation of function is not valid.")
        else:
            self._aggregator = aggregator

        return self

    #----------------------------------------------------------------
    # Naming
    #----------------------------------------------------------------
    @S.builder
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
        self._namer = name
        return self

    @S.builder
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
        self._namer = namer or (lambda x:x)
        return self

    #----------------------------------------------------------------
    # Aggregation
    #----------------------------------------------------------------
    @S.builder
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
        return self.fold(lambda vs: vs[index] if len(vs) > index else alt)

    @S.builder
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
        return self.at(0, alt)

    @S.builder
    def last(self, alt=None):
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
        return self.fold(lambda vs: vs[-1] if len(vs) > 0 else alt)

    @S.builder
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
        return self._set_aggregator(aggregator, True)

    @S.builder
    def select(self, aggregator):
        return self._set_aggregator(aggregator, False)

    #----------------------------------------------------------------
    # Serizlization
    #----------------------------------------------------------------
    @S.builder
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

    @S.builder
    def to(self, **settings):
        def to_dict(c, n, b, v) -> dict:
            vv = b(v)
            return SerializationContext(settings, c.finder).execute(vv.view)
        self._serializers.append(to_dict)
        return self.head()


class SerializationContext:
    """
    This class contains a set of `NodeSerializer`s and the interface to find serialization function registered in `GraphSpec`.
    """
    def __init__(self, settings, finder):
        self.serializer_map = {n:self._to_serializer(s) for n, s in settings.items()}
        self.finder = finder

    def _to_serializer(self, s):
        if isinstance(s, NodeSerializer):
            return s
        else:
            return S.of(*s)

    def execute(self, graph):
        result = {}

        for n, c in filter(lambda nc: nc[1]().property.parent is None, graph):
            self.serialize_to(c().name, c, result)

        return result

    def serialize_to(self, name, container, parent):
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
        ns = self.serializer_map.get(name, None)

        if ns:
            agg = ns.aggregator(container().nodes)

            serializer = ns.serializer

            if ns.be_singular:
                # Alternative value given to aggregation function may be returned instead of node.
                value = self.serialize_node(container().property, agg, serializer) if isinstance(agg, Node) else agg

                if ns.be_merged:
                    if not isinstance(value, dict):
                        raise ValueError(f"Serialized value must be dict but {type(value)}.")

                    parent.update({ns.namer(k):v for k, v in value.items()})
                else:
                    parent[ns.namer(name)] = value
            else:
                if ns.be_merged:
                    raise ValueError(f"Merging to parent dict requires folding.")

                parent[ns.namer(name)] = [self.serialize_node(container().property, n, serializer) for n in agg]

    def serialize_node(self, prop, node, serializer):
        value = serializer(self, node, self.finder(prop.kind), node.entity)

        if isinstance(value, dict):
            for n, ch in node.children.items():
                self.serialize_to(n, ch.view, value)

        return value


def as_is(x):
    return x
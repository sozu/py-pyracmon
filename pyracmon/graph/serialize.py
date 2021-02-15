from inspect import signature, Signature
from typing import TypeVar, get_type_hints
from .template import GraphTemplate
from .graph import Node
from .schema import Shrink, Extend, T, TypedDict, Typeable, GraphSchema
from .util import chain_serializers, wrap_serializer


class S:
    """
    An utility class to build `NodeSerializer`.
    """
    @classmethod
    def of(cls, namer=None, aggregator=None, *serializers):
        """
        Create an instance of `NodeSerializer`.

        Parameters
        ----------
        namer: str | str -> str
            A string or naming function.
        aggregator: [T] -> T | int
            An aggregation function or an index at which the value is selected from converted values in aggregation phase.
        serializer: T -> U
            A list of *serializer*s.

        Returns
        -------
        NodeSerializer
            Created instance.
        """
        return NodeSerializer(namer, aggregator, *serializers)

    @classmethod
    def builder(cls, builder):
        """
        This method is used as decorator to put decorating target to `S` builder methods.

        >>> @S.builder
        >>> def some_func():
        >>>     ...
        >>>
        >>> S.some_func()

        Parameters
        ----------
        builder: callable
            Decorating target.

        Returns
        -------
        callable
            Decorated builder.
        """
        def f(*args, **kwargs):
            return builder(S.of(), *args, **kwargs)
        setattr(S, builder.__name__, f)
        return builder


class NodeSerializer:
    """
    The instance of this class contains the configurations to convert a `NodeContainer` into a serializable value.

    The conversion is composed of 3 phases.

    1. If `aggregator` is set, aggregate the list of nodes into a node or shrinked list of nodes.
    2. Convert the entity of each node into a serializable value by *serializer*.
    3. Put the converted value(s) into the dictionary converted from the parent node with a key determined by `namer`.

    These phases are applied from the roots of graph to their descendants as long as the entity is converted to `dict`.
    Children of the entity which is not converted to `dict` are simply ignored.

    When `namer` is a callable, the result updates parent dictionary with its items instead of just being put to it.

    >>> graph.append(a=dict(a1=1, a2=2), b=dict(b1=3, b2=4))
    >>> GraphSpec().to_dict(
    >>>     graph.view,
    >>>     a = S.of(),
    >>>     b = S.merge(lambda n: f"__{n}__"),
    >>> )
    {"a": [{"a1":1, "a2":2, "__b1__":3, "__b2__":4}]}

    In this case, the property name `b` is ignored and `head()` aggregation is implicitly applied to the property if no aggregator is set.
    As shown in the example, the converted value on `b` must be a `dict` in order to being expanded to key-value pairs.
    Exception raises when the value of another type is obtained.
    """
    def __init__(self, namer=None, aggregator=None, *serializers):
        self._namer = namer
        self._aggregator = aggregator
        self._serializers = list(serializers)
        self._doc = ""

    @property
    def namer(self):
        """
        Naming function.

        Returns
        -------
        str -> str
            A function which determines the key of the property even when `namer` is set to a string.
        """
        if not self._namer:
            return lambda n: n
        elif isinstance(self._namer, str):
            return lambda n: self._namer
        else:
            return lambda n: self._namer(n)

    @property
    def aggregator(self):
        """
        Aggregation function.

        Returns
        -------
        [Node] -> Node | [Node]
            A function to aggregate a list of nodes to a node or another list of nodes.
        """
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
        """
        Serialization function.

        Returns
        -------
        (SerializationContext, Node, object -> object, object) -> object
            A function to convert an entity value into a serializable value.
        """
        return chain_serializers(self._serializers)

    @property
    def be_merged(self):
        """
        Returns whether the converted value will be merged into parent.

        Returns
        -------
        bool
            `True` if converted value will be merged into parent.
        """
        return callable(self._namer)

    @property
    def be_singular(self):
        """
        Returns whether the converted value will be a singular object, not a list.

        This is estimated by annotation of aggregation function. If its returning type is not annotated, this property always returns `False`.
        `S` builder methods adds appropriate annotation to given function when it does not have the annotation.

        Returns
        -------
        bool
            `True` if converted value will be a singular object. `False` means that it will be a list.
        """
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
    # Documentation
    #----------------------------------------------------------------
    @S.builder
    def doc(self, document):
        """
        Set the documentation for this node.

        Parameters
        ----------
        document: str
            A documentation string.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self._doc = document
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
        Set a naming function taking a property name and returning a key in parent `dict`.

        `NodeSerializer` built with this methods merges the converted `dict` into parent `dict` and its `be_merged` property becomes `True`.

        Because merging needs folding, this method overrides the aggregation function by invoking `head()` internally
        if this serializer is not configured to fold nodes into a single node.

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
        if not self.be_singular:
            self.head()
        return self

    #----------------------------------------------------------------
    # Aggregation
    #----------------------------------------------------------------
    @S.builder
    def at(self, index, alt=None):
        """
        Set an aggregator which picks up the node at the index.

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
        Set an aggregator which picks up the first node.

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
        Set an aggregator which picks up the last node.

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
        Set an aggregation function converting a list of nodes into a single node.

        Parameters
        ----------
        aggregator: [Node] -> Node
            An aggregation function.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        return self._set_aggregator(aggregator, True)

    @S.builder
    def select(self, aggregator):
        """
        Set an aggregation function chooding a list of nodes from all nodes in `NodeContainer`.

        Parameters
        ----------
        aggregator: [Node] -> [Node]
            An aggregation function.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        return self._set_aggregator(aggregator, False)

    #----------------------------------------------------------------
    # Serizlization
    #----------------------------------------------------------------
    @S.builder
    def each(self, func):
        """
        Set a function converting a node entity into a serializable value.

        In order to progress serialization to child nodes, the function MUST returns a `dict`.

        The function will be invoked with 0 to 4 arguments listed below.

        - `SerializationContext` of the serialization.
        - `Node` to serialize.
        - A function which takes the entity value and returns converted value by applying all serialization functions added beforehand.
        - An entity value to convert.

        When the number of arguments in signature of the function is less than 4, former arguments is the list are omitted.
        For example, `def func(c, n, b, v):` is invoked with all arguments, while `def func(b, v):` is invoked with only 3rd and 4th arguments.

        Parameters
        ----------
        func: (SerializationContext, Node, object -> object, object) -> object
            A function converting a node entity into a serializable value.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        self._serializers.append(func)
        return self

    @S.builder
    def sub(self, **settings):
        """
        Set serialization settings to serializer sub graph.

        This method is used for the property whose kind is `GraphTemplate`.
        `settings` keyword arguments should be the same form of the keyword arguments of `to_dict()` of `GraphSpec`.

        Parameters
        ----------
        settings: {str: NodeSerializer}
            Serialization settings used to serialize sub graph.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        class SubGraph(Typeable[T]):
            serializers = settings.copy()

            @staticmethod
            def resolve(sub_graph, bound, arg, spec):
                return GraphSchema(spec, arg.template, **sub_graph.serializers).schema

        def to_dict(c, n, b, v) -> SubGraph[T]:
            vv = b(v)
            return SerializationContext(settings, c.finder).execute(vv.view)
        self._serializers.append(to_dict)
        return self.head()

    @S.builder
    def alter(self, generator=None, excludes=None, includes=None):
        """
        Extends and shrinks the dictionary.

        Parameters
        ----------
        generator: (SerializationContext, Node, object -> object, object) -> {str: object}
            A function generating dictionary
        excludes: Iterable[str]
            Keys to exclude from the dictionary.
        includes: Iterable[str]
            Keys to keep in the dictionary.

        Returns
        -------
        NodeSerializer
            This instance.
        """
        excludes = excludes or []

        class EachExtend(Extend[T]):
            @classmethod
            def schema(cls, bound, arg):
                return signature(generator).return_annotation if generator else Signature.empty

        class EachShrink(Shrink[T]):
            @classmethod
            def select(cls, td, bound):
                return excludes, includes

        def convert(c, n, b, v) -> EachShrink[EachExtend[T]]:
            ext = wrap_serializer(generator)(c, n, b, v) if generator else {}
            vv = b(v)
            vv.update(**ext)
            return {k:v for k, v in vv.items() if (not includes or k in includes) and k not in excludes}

        self._serializers.append(convert)
        return self


class SerializationContext:
    """
    This class provides a functionality to serialize a graph by using containing `NodeSerializer` s.
    """
    def __init__(self, settings, finder, node_params=None):
        self.serializer_map = {n:self._to_serializer(s) for n, s in settings.items()}
        self.finder = finder
        self.node_params = node_params or {}

    def __getitem__(self, node):
        """
        Returns an accessor to parameters for given node.

        Parameters
        ----------
        node: Node | str
            Node or node name.

        Returns
        -------
        Accessor
            An object exposing parameters via its attributes of their names.
        """
        name = node.name if isinstance(node, Node) else node
        params = self.node_params.get(name, {})

        class Accessor:
            def __getattr__(self, key):
                return params.get(key, None)

        return Accessor()

    def _to_serializer(self, s):
        if isinstance(s, NodeSerializer):
            return s
        else:
            return S.of(*s)

    def execute(self, graph):
        """
        Serializes a graph.

        Parameters
        ----------
        graph: GraphView
            The view of graph to serialize.

        Returns
        -------
        dict
            Serialization result.
        """
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
        container: NodeContainer | Node.Children
            Container of nodes. 
        parent: dict
            A parent dictionary the converted values are appended to.
        """
        ns = self.serializer_map.get(name, None)

        if ns:
            agg = ns.aggregator(container().nodes)

            serializer = ns.serializer

            if ns.be_singular:
                # Alternative value given to aggregation function may be returned instead of node.
                value = self._serialize_node(container().property, agg, serializer) if isinstance(agg, Node) else agg

                if ns.be_merged:
                    if not isinstance(value, dict):
                        raise ValueError(f"Serialized value must be dict but {type(value)}.")

                    parent.update({ns.namer(k):v for k, v in value.items()})
                else:
                    parent[ns.namer(name)] = value
            else:
                if ns.be_merged:
                    raise ValueError(f"Merging to parent dict requires folding.")

                parent[ns.namer(name)] = [self._serialize_node(container().property, n, serializer) for n in agg]

    def _serialize_node(self, prop, node, serializer):
        value = serializer(self, node, chain_serializers(self.finder(prop.kind)), node.entity)

        if isinstance(value, dict):
            for n, ch in node.children.items():
                self.serialize_to(n, ch.view, value)

        return value
from inspect import signature, Signature
from typing import *
from .template import GraphTemplate
from .graph import Node, NodeContainer
from .schema import Shrink, Extend, T, TypedDict, Typeable, GraphSchema, issubgeneric
from .util import Serializer, chain_serializers, wrap_serializer


class S:
    """
    An utility class to build `NodeSerializer`.
    """
    @classmethod
    def of(
        cls,
        namer: Optional[Union[str, Callable[[str], str]]] = None,
        aggregator: Optional[Union[Callable[[Node], Node], Callable[[Node], List[Node]], int]] = None,
        *serializers: Serializer,
    ) -> 'NodeSerializer':
        """
        Create an instance of `NodeSerializer`.

        :param namer: A string or naming function.
        :param aggregator: An aggregation function or an index of node to select in node container.
        :param serializer: A list of *serializer* s.
        :returns: Created `NodeSerializer` .
        """
        return NodeSerializer(namer, aggregator, *serializers)

    @classmethod
    def builder(cls, builder: Callable[[Any], Any]):
        """
        This method is used as decorator to put decorating target to `S` builder methods.

        >>> @S.builder
        >>> def some_func():
        >>>     ...
        >>> S.some_func()

        :param builder: Decorating target.
        :returns: Decorated builder.
        """
        def f(*args, **kwargs):
            return builder(S.of(), *args, **kwargs)
        setattr(S, builder.__name__, f)
        return builder


class NodeSerializer:
    """
    The instance of this class contains the configurations to conert a `NodeContainer` into values in serialization result.

    The conversion is composed of 3 phases.

    1. If ``aggregator`` is set, aggregate the list of nodes into a node or shrinked list of nodes.
    2. Convert the entity of each node into a serializable value by *serializer* .
    3. Put the converted value(s) into the dictionary converted from the parent node with a key determined by ``namer``.

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

    In this case, the property name ``b`` is ignored and `head` aggregation is implicitly applied to the property if no aggregator is set.
    As shown in the example, the converted value on ``b`` must be a `dict` in order to being expanded to key-value pairs.
    Exception raises when the value of another type is obtained.

    :param namer: Key in the dictionary or naming funciton.
    :param aggregator: Function to select node(s) from the node container.
    :param serializers: List of *serializer* s.
    """
    def __init__(
        self,
        namer: Optional[Union[str, Callable[[str], str]]] = None,
        aggregator: Optional[Union[Callable[[Node], Node], Callable[[Node], List[Node]], int]] = None,
        *serializers: Serializer,
    ):
        self._namer = namer
        self._aggregator = aggregator
        self._serializers = list(serializers)
        self._doc = ""

    @property
    def namer(self) -> Callable[[str], str]:
        """
        Returns naming function which determines the key of the property even when `namer` is set to a string.

        :getter: Naming function.
        """
        if not self._namer:
            return lambda n: n
        elif isinstance(self._namer, str):
            return lambda n: self._namer
        else:
            return lambda n: self._namer(n)

    @property
    def aggregator(self) -> Union[Callable[[Node], Node], Callable[[Node], List[Node]]]:
        """
        Returns aggregation function which selects node(s) from nodes in the container.

        :getter: Aggregation function.
        """
        if self._aggregator is None:
            def agg(values: List[T]) -> List[T]:
                return values
            return agg
        elif signature(self._aggregator).return_annotation == Signature.empty:
            def agg(values: List[T]) -> List[T]:
                return self._aggregator(values)
            return agg
        else:
            return self._aggregator

    @property
    def serializer(self) -> Serializer:
        """
        Returns serialization function which converts an entity into a serializable value.

        :getter: Serialization function.
        """
        return chain_serializers(self._serializers)

    @property
    def be_merged(self) -> bool:
        """
        Returns whether the converted value will be merged into parent.

        :getter: ``True`` if converted value will be merged into parent.
        """
        return callable(self._namer)

    @property
    def be_singular(self) -> bool:
        """
        Returns whether the converted value will be a singular object, not a list.

        This is estimated by annotation of aggregation function. If its returning type is not annotated, this property always returns ``False`` .
        `S` builder methods adds appropriate annotation to given function when it does not have the annotation.

        :getter: ``True`` if converted value will be a singular object. ``False`` means that it will be a list.
        """
        #return not isinstance(rt, list)
        rt = signature(self.aggregator).return_annotation
        return not issubgeneric(rt, list)

    def _set_aggregator(self, aggregator, folds):
        try:
            rt = signature(aggregator).return_annotation
        except:
            rt = Signature.empty

        if rt == Signature.empty:
            def agg(vs: List[T]) -> (T if folds else List[T]):
                return aggregator(vs)
            self._aggregator = agg
        elif issubgeneric(rt, list) ^ (not folds):
            raise ValueError(f"Return annotation of function is not valid.")
        else:
            self._aggregator = aggregator

        return self

    #----------------------------------------------------------------
    # Documentation
    #----------------------------------------------------------------
    @S.builder
    def doc(self, document: str) -> 'NodeSerializer':
        """
        Set the documentation for this node.

        :param document: A documentation string.
        :returns: This instance.
        """
        self._doc = document
        return self

    #----------------------------------------------------------------
    # Naming
    #----------------------------------------------------------------
    @S.builder
    def name(self, name: str) -> 'NodeSerializer':
        """
        Set a key in parent dictionary.

        :param name: A key string.
        :returns: This instance.
        """
        if not isinstance(name, str):
            raise ValueError(f"The name of node must be a string but {type(name)} is given.")
        self._namer = name
        return self

    @S.builder
    def merge(self, namer: Optional[Union[str, Callable[[str], str]]] = None) -> 'NodeSerializer':
        """
        Set a naming function taking a property name and returning a key in parent `dict`.

        `NodeSerializer` built with this methods merges the converted `dict` into parent `dict` and its `be_merged` property becomes ``True`` .

        Because merging needs folding, this method overrides the aggregation function by invoking `head()` internally
        if this *serializer* is not configured to fold nodes into a single node.

        :param namer: The naming function. If ``None`` , the property name is returned as it is.
        :returns: This instance.
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
    def at(self, index: int, alt: Any = None) -> 'NodeSerializer':
        """
        Set an aggregator which picks up the node at the index.

        :param index: An index of the element.
        :param alt: A value used when no node is found at the index.
        :returns: This instance.
        """
        return self.fold(lambda vs: vs[index] if len(vs) > index else alt)

    @S.builder
    def head(self, alt: Any = None) -> 'NodeSerializer':
        """
        Set an aggregator which picks up the first node.

        :param alt: A value used when there exists no nodes.
        :returns: This instance.
        """
        return self.at(0, alt)

    @S.builder
    def last(self, alt: Any = None) -> 'NodeSerializer':
        """
        Set an aggregator which picks up the last node.

        :param alt: A value used when there exists no nodes.
        :returns: This instance.
        """
        return self.fold(lambda vs: vs[-1] if len(vs) > 0 else alt)

    @S.builder
    def fold(self, aggregator: Callable[[List[Node]], Node]) -> 'NodeSerializer':
        """
        Set an aggregation function converting a list of nodes into a single node.

        :param aggregator: An aggregation function.
        :returns: This instance.
        """
        return self._set_aggregator(aggregator, True)

    @S.builder
    def select(self, aggregator: Callable[[List[Node]], List[Node]]) -> 'NodeSerializer':
        """
        Set an aggregation function chooding a list of nodes from all nodes in `NodeContainer` .

        :param aggregator: An aggregation function.
        :returns: This instance.
        """
        return self._set_aggregator(aggregator, False)

    #----------------------------------------------------------------
    # Serizlization
    #----------------------------------------------------------------
    @S.builder
    def each(self, func: Serializer) -> 'NodeSerializer':
        """
        Set a function converting a node entity into a serializable value.

        In order to progress serialization to child nodes, the function MUST returns a `dict` .

        The function (i.e. *serializer* ) will be invoked with 0 to 4 arguments listed below.

        - `SerializationContext` of the serialization.
        - `Node` to serialize.
        - A function which takes the entity value and returns converted value by applying all serialization functions added beforehand.
        - An entity value to convert.

        When the number of arguments in signature of the function is less than 4, former arguments are omitted.
        For example, ``def func(c, n, b, v):`` is invoked with all arguments, while ``def func(b, v):`` is invoked with only 3rd and 4th arguments.

        :param func: A function converting a node entity into a serializable value.
        :returns: This instance.
        """
        self._serializers.append(func)
        return self

    @S.builder
    def sub(self, **settings) -> 'NodeSerializer':
        """
        Set serialization settings for sub graph.

        This method is used for the property whose kind is `GraphTemplate` .
        The form of ``settings`` is same as keyword arguments of `GraphSpec.to_dict` .

        :param settings: Serialization settings used to serialize sub graph.
        :returns: This instance.
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
    def alter(
        self,
        generator: Optional[Serializer] = None,
        excludes: Optional[List[str]] = None,
        includes: Optional[List[str]] = None,
    ) -> 'NodeSerializer':
        """
        Extends and shrinks the dictionary.

        ``generator`` is a kind of *serializer* returning `dict` which will be merged into serialization result.
        ``excludes`` and ``includes`` are used to select keys in the result.

        :param generator: A function generating dictionary to be merged.
        :param excludes: Keys to exclude.
        :param includes: Keys to keep.
        :returns: This instance.
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
    This class implements actual serialization flow using `NodeContainer` s.

    `node_params` enables *serializer* to get values dynamically determined
    because they are obtained by this context which can be obtained at the first argument of *serializer* function.

    Following code shows the example using the parameters in *serializer* .

    >>> cxt = SerializationContext(
    >>>     dict(
    >>>         a = S.each(lambda c,n,b,v: v*c["a"].value),
    >>>     ),
    >>>     finder,
    >>>     dict(a={"value": 10})
    >>> )

    :param settings: Mapping of node name to `NodeSerializer` .
    :param finder: A function to find base *serializer* s by a `type` .
    :param node_params: Parameters bound to node name. This context works as this `dict` on index access.
    """
    def __init__(
        self,
        settings: Dict[str, NodeSerializer],
        finder: Callable[[type], List[Serializer]],
        node_params: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.serializer_map = {n:self._to_serializer(s) for n, s in settings.items()}
        self.finder = finder
        self.node_params = node_params or {}

    def __getitem__(self, node: Union[Node, str]) -> Any:
        """
        Returns an accessor to parameters for given node.

        :param node: Node or node name.
        :returns: An object exposing parameters for the node as attributes.
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

    def execute(self, graph: 'GraphView') -> Dict[str, Any]:
        """
        Serializes a graph.

        :param graph: The view of graph to serialize.
        :returns: Serialization result.
        """
        result = {}

        for n, c in filter(lambda nc: nc[1]().prop.parent is None, graph):
            self.serialize_to(c().name, c, result)

        return result

    def serialize_to(self, name: str, container: Union[NodeContainer, Node.Children], parent: Dict[str, Any]):
        """
        Serialize nodes and appends them into the dictionary.

        :param name: Name of the template property associated with the nodes.
        :param container: Container of nodes. 
        :param parent: A parent dictionary to which serialized values will be appended.
        """
        ns = self.serializer_map.get(name, None)

        if ns:
            agg = ns.aggregator(container().nodes)

            serializer = ns.serializer

            if ns.be_singular:
                # Alternative value given to aggregation function may be returned instead of node.
                value = self._serialize_node(container().prop, agg, serializer) if isinstance(agg, Node) else agg

                if ns.be_merged:
                    if not isinstance(value, dict):
                        raise ValueError(f"Serialized value must be dict but {type(value)}.")

                    parent.update({ns.namer(k):v for k, v in value.items()})
                else:
                    parent[ns.namer(name)] = value
            else:
                if ns.be_merged:
                    raise ValueError(f"Merging to parent dict requires folding.")

                parent[ns.namer(name)] = [self._serialize_node(container().prop, n, serializer) for n in agg]

    def _serialize_node(self, prop, node, serializer):
        value = serializer(self, node, chain_serializers(self.finder(prop.kind)), node.entity)

        if isinstance(value, dict):
            for n, ch in node.children.items():
                self.serialize_to(n, ch.view, value)

        return value
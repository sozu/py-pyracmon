from collections.abc import Iterator, Iterable
from inspect import signature, Signature, getmembers, isfunction
from typing import Any, Mapping, Callable, Protocol, TypeVar, cast
from .template import GraphTemplate
from .graph import Node, NodeContainer, GraphView
from .typing import Shrink, Extend, Typeable, issubgeneric, to_rawdict


_T = TypeVar('_T')


#: Type alias for serialization function.
type Serializer = Callable[['NodeContext'], Any]


class NodeSerializing(Protocol):
    def doc(self, document: str, **options: Any) -> 'NodeSerializer': ...
    def name(self, name: str) -> 'NodeSerializer': ...
    def merge(self, namer: Callable[[str], str] | None = None) -> 'NodeSerializer': ...
    def at(self, index: int, alt: Any = None) -> 'NodeSerializer': ...
    def head(self, alt: Any = None) -> 'NodeSerializer': ...
    def last(self, alt: Any = None) -> 'NodeSerializer': ...
    def fold(self, aggregator: Callable[[list[Node]], Any]) -> 'NodeSerializer': ...
    def select(self, aggregator: Callable[[list[Node]], list[Node]]) -> 'NodeSerializer': ...
    def each(self, func: Serializer) -> 'NodeSerializer': ...
    def sub(self, **settings: 'NodeSerializer') -> 'NodeSerializer': ...
    def alter(
        self,
        generator: Serializer | None = None,
        excludes: Iterable[str] | None = None,
        includes: Iterable[str] | None = None,
    ) -> 'NodeSerializer': ...
    def nest(self, **serializers: 'NodeSerializer') -> 'NodeSerializer': ...


class NodeSerializer(NodeSerializing):
    """
    This class provides methods to configure serialization of a node container.

    Graph is serialized into `dict` from root node containers to their descendants.
    `NodeSerializer` should be set to each container (= template property) to control how to serialize nodes in it.

    At first, nodes to be serialized are selected from a node container using *aggregator*
    which is a function or sequence of functions extracting a node or nodes.
    `fold` and `select` are the general methods to set *aggregator* to `NodeSerializer` .

    ```python
    >>> # NodeSerializer to select first node in the container.
    >>> S.fold(lambda ns: ns[0])
    >>> # NodeSerializer to select every other node.
    >>> S.select(lambda ns: ns[0::2])
    ```

    Each selected node is serialized in the way determined by the type of corresponding template property.
    Serialization function (= *serializer* ) is obtained usually from `pyracmon.graph.spec.GraphSpec`
    which stores *serializer*s in association with types.
    Additionally, *serializer*s can be set to `NodeSerializer` directly by `each` or some other methods.
    For each node, all *serializer*s are collected and merged into a function which finally is applied to its entity.

    ```python
    >>> spec = GraphSpec()
    >>> # Register a serializer for int type which converts an int into a dict.
    >>> spec.add_serializer(int, lambda v: dict(v=v))
    >>> # Set serializer which multiplies values in dict.
    >>> ns = S.each(lambda cxt: {k:v*2 for k,v in cxt.serialize()})
    >>> # Do serialization
    >>> graph.append(a=1).append(a=2)
    >>> spec.to_dict(
    >>>     graph.view,
    >>>     a = ns,
    >>> )
    {"a": [{"v": 2}, {"v": 4}]}
    ```

    By default, child nodes are also serialized and put into the serialized parent node only when the parent node is serialized into `dict` .
    Serialized value of each child node is put into the `dict` with the same key as the property name.
    The key can be changed by set *namer* to the `NodeSerializer` by `name` .

    Here, `merge` is a special configuration of `NodeSerializer` available only when the node is serialized into `dict` .
    The child `dict` is merged into parent `dict` in the same way as `update` method of `dict` .
    In this case, the key of child `dict` can be changed by a callable passed to `merge` as follows.

    ```python
    >>> graph.append(a=dict(a1=1, a2=2), b=dict(b1=3, b2=4))
    >>> GraphSpec().to_dict(
    >>>     graph.view,
    >>>     a = S.of(),
    >>>     b = S.merge(lambda n: f"__{n}__"),
    >>> )
    {"a": [{"a1":1, "a2":2, "__b1__":3, "__b2__":4}]}
    ```
    """
    def __init__(
        self,
        namer: str | Callable[[str], str] | None = None,
        aggregator: Callable[[list[Node]], Node] | Callable[[list[Node]], list[Node]] | None = None,
        *serializers: Serializer,
    ):
        """
        Create an instance.

        Args:
            namer: A string or a function determining the key in parent `dict` .
            aggregator: A function to select node(s) from the node container.
            serializers: List of *serializer* s.
        """
        self._namer = namer
        self._aggregator = aggregator
        self._serializers = list(serializers)
        self._nested_serializers: dict[str, NodeSerializer] = {}
        self._be_merged = False
        self._doc = ""
        self._doc_options = {}

    @property
    def namer(self) -> Callable[[str], str]:
        """
        Returns *namer* in the form of function even when not to merge.
        """
        def f(v: str) -> str:
            if self._namer is None:
                return v
            elif isinstance(self._namer, str):
                return self._namer
            else:
                return self._namer(v)
        return f

    @property
    def aggregator(self) -> Callable[[list[Node]], list[Node] | Node | Any]:
        """
        Returns *aggregator* supplied with correct return annotation.
        """
        if self._aggregator is None:
            def agg1(values: list[_T]) -> list[_T]:
                return values
            #agg1.__annotations__['return'] = list[_T] # type: ignore
            return agg1
        elif signature(self._aggregator).return_annotation == Signature.empty:
            # TODO: No return annotation implies list to list aggregation.
            def agg2(values: list[_T]) -> list[_T]:
                return self._aggregator(values) # type: ignore
            #agg2.__annotations__['return'] = list[_T] # type: ignore
            return agg2
        else:
            return self._aggregator

    @property
    def serializer(self) -> Serializer:
        """
        Returns merged *serializer* which has correctly annotated signature.
        """
        return chain_serializers(self._serializers)

    @property
    def be_merged(self) -> bool:
        """
        Returns whether the converted value will be merged into parent.
        """
        return self._be_merged

    @property
    def be_singular(self) -> bool:
        """
        Returns whether the converted value will be a singular object, not a list.

        This is estimated by annotation of aggregation function. If its returning type is not annotated, this property always returns `False` .
        Builder methods adds appropriate annotation to given function when it does not have the annotation.
        """
        rt = signature(self.aggregator).return_annotation
        return not issubgeneric(rt, list)

    def _set_aggregator(self, aggregator, folds):
        try:
            rt = signature(aggregator).return_annotation
        except:
            rt = Signature.empty

        if rt == Signature.empty:
            def agg(vs: list[_T]):
                return aggregator(vs)
            agg.__annotations__['return'] = _T if folds else list[_T] # type: ignore
            self._aggregator = agg
        elif issubgeneric(rt, list) ^ (not folds):
            raise ValueError(f"Return annotation of function is not valid.")
        else:
            self._aggregator = aggregator

        return self

    #----------------------------------------------------------------
    # Documentation
    #----------------------------------------------------------------
    def doc(self, document: str, **options: Any) -> 'NodeSerializer':
        """
        Set the documentation for this node.

        `document` is used in graph schema as a parameter of `Annotated` .

        Args:
            document: A documentation string.
            options: Documentation options.
        Returns:
            This instance.
        """
        self._doc = document
        self._doc_options = options
        return self

    #----------------------------------------------------------------
    # Naming
    #----------------------------------------------------------------
    def name(self, name: str) -> 'NodeSerializer':
        """
        Set a key in parent dictionary.

        Args:
            name: A key string.
        Returns:
            This instance.
        """
        if not isinstance(name, str):
            raise ValueError(f"The name of node must be a string but {type(name)} is given.")
        self._namer = name
        self._be_merged = False
        return self

    def merge(self, namer: Callable[[str], str] | None = None) -> 'NodeSerializer':
        """
        Set a naming function for merging into parent.

        Because merging needs folding, this method overrides the aggregation function by invoking `head()` internally
        if this instance is not configured to fold nodes into a single node.

        Args:
            namer: The naming function. If `None`, the property is used as it is.
        Returns:
            This instance.
        """
        if namer and not callable(namer):
            raise ValueError(f"The method merging a node into its parent node must be callable or None.")
        self._namer = namer or (lambda x:x)
        self._be_merged = True
        if not self.be_singular:
            self.head()
        return self

    #----------------------------------------------------------------
    # Aggregation
    #----------------------------------------------------------------
    def at(self, index: int, alt: Any = None) -> 'NodeSerializer':
        """
        Set an *aggregator* which picks up the node at the index.

        Args:
            index: An index of the element.
            alt: A value used when no node is found at the index.
        Returns:
            This instance.
        """
        if alt is None:
            def agg_opt(vs: list[_T]) -> _T | None:
                return vs[index] if len(vs) > index else alt
            #agg_opt.__annotations__['return'] = _T | None # type: ignore
            return self.fold(agg_opt)
        else:
            def agg(vs: list[_T]) -> _T:
                return vs[index] if len(vs) > index else alt
            #agg.__annotations__['return'] = _T # type: ignore
            return self.fold(agg)

    def head(self, alt: Any = None) -> 'NodeSerializer':
        """
        Set an *aggregator* which picks up the first node.

        Args:
            alt: A value used when there exists no nodes.
        Returns:
            This instance.
        """
        return self.at(0, alt)

    def last(self, alt: Any = None) -> 'NodeSerializer':
        """
        Set an *aggregator* which picks up the last node.

        Args:
            alt: A value used when there exists no nodes.
        Returns:
            This instance.
        """
        def agg(vs: list[_T]) -> ((_T | None) if alt is None else _T): # type: ignore
            return vs[-1] if len(vs) > 0 else alt
        return self.fold(agg)

    def fold(self, aggregator: Callable[[list[Node]], Any]) -> 'NodeSerializer':
        """
        Set an aggregation function converting a list of nodes into a single node or any value.

        Args:
            aggregator: An aggregation function.
        Returns:
            This instance.
        """
        return self._set_aggregator(aggregator, True)

    def select(self, aggregator: Callable[[list[Node]], list[Node]]) -> 'NodeSerializer':
        """
        Set an aggregation function selecting a list of nodes from all nodes from the container.

        Args:
            aggregator: An aggregation function.
        Returns:
            This instance.
        """
        return self._set_aggregator(aggregator, False)

    #----------------------------------------------------------------
    # Serizlization
    #----------------------------------------------------------------
    def each(self, func: Serializer) -> 'NodeSerializer':
        """
        Set a *serializer* .

        *serializer* is a function which will be invoked with a single argument of `NodeContext` ,
        from which internal code of *serializer* can get information of the node.

        For the sake of static typing, the *serializer* should have correct return annotation.

        Args:
            func: A function converting a node entity into a value.
        Returns:
            This instance.
        """
        self._serializers.append(func)
        return self

    def sub(self, **settings: 'NodeSerializer') -> 'NodeSerializer':
        """
        Set serialization settings for sub graph.

        This method is used for the property whose kind is `GraphTemplate` .
        The form of `settings` is same as keyword arguments used to serialize the graph.

        Args:
            settings: Serialization settings used to serialize sub graph.
        Returns:
            This instance.
        """
        from pyracmon.graph.schema import GraphSchema
        class SubGraph[_T](Typeable[_T]):
            serializers = settings.copy()

            @staticmethod
            def resolve(sub_graph, bound, arg, spec):
                return GraphSchema(spec, arg.template, **sub_graph.serializers).schema

        def to_dict(cxt: NodeContext) -> SubGraph[_T]:
            vv = cxt.serialize()
            return SerializationContext(
                settings,
                cxt.context.finder if cxt.context else lambda t: [],
            ).execute(vv.view) # type: ignore

        self._serializers.append(to_dict)
        return self.head()

    def alter(
        self,
        generator: Serializer | None = None,
        excludes: Iterable[str] | None = None,
        includes: Iterable[str] | None = None,
    ) -> 'NodeSerializer':
        """
        Extends and shrinks the dictionary obtained as a result of *serializer*s applied beforehand.

        `generator` is a kind of *serializer* returning `dict` which will be merged into serialization result.
        This can be used to add extra key value pairs into the result.
        For the sake of static typing, `generator` should have correct return annotation of `TypedDict` .

        `excludes` and `includes` are used to select keys from the result.

        Args:
            generator: A function generating dictionary to be merged.
            excludes: Keys to exclude.
            includes: Keys to keep.
        Returns:
            This instance.
        """
        excludes = excludes or []

        class EachExtend(Extend[_T]):
            @classmethod
            def schema(cls, bound, arg):
                return signature(generator).return_annotation if generator else Signature.empty

        class EachShrink(Shrink[_T]):
            @classmethod
            def select(cls, td, bound):
                return excludes, includes

        def convert(cxt) -> EachShrink[EachExtend[_T]]:
            ext = generator(cxt) if generator else {}
            vv = cxt.serialize()
            vv.update(**to_rawdict(ext, True))
            return {k:v for k, v in vv.items() if (not includes or k in includes) and k not in excludes} # type: ignore

        self._serializers.append(convert)
        return self

    def nest(self, **serializers: 'NodeSerializer') -> 'NodeSerializer':
        """
        Set serialization settings for nested graph.

        Args:
            serializers: Serialization settings used to serialize nested graph.
        Returns:
            This instance.
        """
        self._nested_serializers.update(serializers)
        return self


class NodeParams:
    def __init__(self, params) -> None:
        self._params: dict[str, Any] = params

    def __getitem__(self, key: str):
        """
        Returns a value by key from values passed from invoking scope being bound for the node.
        """
        return self._params.get(key, None)

    def __getattr__(self, key: str) -> Any | None:
        """
        Returns a value by key from values passed from invoking scope being bound for the node.
        """
        return self._params.get(key, None)


class NodeContext:
    """
    Represents a context for serialization of a node.

    The instance of this class appears as an argument of serialization function
    in order to implement the function by using information of the node seriazlization inside the function.

    Properties listed below are available:

    - `context` : `SerializationContext` for the serialization of the graph.
    - `node` : `Node` to serialize.
    - `value` : Entity value of the `Node` .
    - `params` : Arbitrary values which is passed from invoking scope with being bound to the key of node name.

    In addition, you can get the result of preceding serializers by calling `serialize`.
    """
    def __init__(self, context: 'SerializationContext', params: NodeParams) -> None:
        #: `SerializationContext` for the serializaion of the graph.`
        self.context = context
        #: Arbitrary values passed by outside for the node.
        self.params = params
        # Set on demand.
        self._node: Node | None = None
        self._iterator: Iterator[Serializer] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._node = None
        self._iterator = None

    @property
    def node(self) -> Node:
        """
        A node to serialize.
        """
        # Node must be set when passed to serialization function.
        return cast(Node, self._node)

    @property
    def value(self) -> Any:
        """
        An entity value of the node to serialize.
        """
        return cast(Node, self._node).entity

    def serialize(self) -> Any:
        """
        Obtain a value serialized by preceeding serializers.

        Returns:
            A value serialized by preceding serializers.
        """
        try:
            return next(cast(Iterator[Serializer], self._iterator))(self)
        except StopIteration:
            return self.node.entity


class NodeContextFactory:
    """
    Factory class to generate a `NodeContext` for a node.
    """
    def __init__(
        self,
        context: 'SerializationContext',
        serializers: list[Serializer],
        params: dict[str, Any],
    ) -> None:
        """
        Create an instance.

        Args:
            context: `SerializationContext` for the serialization of a graph.
            serializers: Globally registered serializers for the type of node entity.
            params: Parameters given at the serialization by caller.
        """
        self.serializers = serializers
        # Generate and keep an instance of NodeContext to save memory for big graph.
        self.node_context = NodeContext(context, NodeParams(params))

    def begin(self, node: Node, serializers: list[Serializer]) -> NodeContext:
        """
        Create a `NodeContext` for a node.

        Args:
            node: A node to serialize.
            serializers: Serializers registered for the node.
        Returns:
            A `NodeContext` for the node.
        """
        self.node_context._node = node
        self.node_context._iterator = iter((self.serializers + serializers)[::-1])
        return self.node_context


class SerializationContext:
    """
    This class implements actual serialization flow applied to a graph.

    `node_params` is a mapping of node name to parameters for the node.
    It should be passed when starting serialization and can be accessed via `params` property of `NodeContext` in serialization function.
    The type of the property value is `NodeParams` which is a wrapper of `dict` and exposes values as attributes.

    ```python
    cxt = SerializationContext(
        dict(
            a = S.each(lambda cxt: cxt.value*cxt.params.key),
        ),
        finder,
        dict(a={"key": 10})
    )
    ```

    In above example, third argument is the `node_params` which contains parameters for node with name `a` .
    The parameters are accessed in the serialization function via `cxt.params` whose type is `NodeParams` .
    """
    def __init__(
        self,
        settings: dict[str, NodeSerializer],
        finder: Callable[[type], list[Serializer]],
        node_params: dict[str, dict[str, Any]] | None = None,
    ):
        """
        Create an instance.

        Args:
            settings: Mapping of node name to `NodeSerializer` .
            finder: A function to find base *serializer* s by a `type` .
            node_params: Arbitrary parameters passed to *serializer*s.
        """
        self.settings: dict[str, NodeSerializer] = settings
        self.finder: Callable[[type], list[Serializer]] = finder
        self._node_params: dict[str, dict[str, Any]] = node_params or {}
        self._context_factories: dict[str, NodeContextFactory] = {}

    def __getitem__(self, node: Node | str) -> Any:
        """
        Returns an accessor to parameters for given node.

        Args:
            node: Node or node name.
        Returns:
            An object exposing parameters for the node as attributes.
        """
        name = node.name if isinstance(node, Node) else node
        params = self._node_params.get(name, {})

        class Accessor:
            def __getattr__(self, key):
                return params.get(key, None)

        return Accessor()

    def execute(self, graph: GraphView) -> dict[str, Any]:
        """
        Serializes a graph.

        Args:
            graph: The view of graph to serialize.
        Returns:
            Serialization result.
        """
        result = {}
        for c in graph().roots:
            self.serialize_to(c.prop.key, c, result)
        return result

    def serialize_to(self, name: str, container: NodeContainer | Node.Children, parent: dict[str, Any]) -> None:
        """
        Serialize nodes and appends them into the dictionary.

        Args:
            name: Name of the template property associated with the nodes.
            container: Container of nodes to serialize. 
            parent: A parent dictionary to which serialized values will be appended.
        """
        ns = self.settings.get(name, None)

        if not ns:
            # Nodes whose names are not supplied to settings are not serialized.
            return

        # First, aggregate nodes into its subset or a single node.
        nodes: list[Node] | Node | Any = ns.aggregator(container.nodes)

        if ns.be_singular:
            if isinstance(nodes, list):
                raise ValueError(f"Aggregation function is marked to create a single value but returns node list.")

            # Alternative value given to aggregation function may be returned instead of node.
            value = self._serialize_node(nodes, ns) if isinstance(nodes, Node) else nodes

            if ns.be_merged:
                if value is None:
                    # When empty, no key-value pair is added to parent.
                    return
                elif not isinstance(value, dict):
                    raise ValueError(f"Serialized value must be dict but {type(value)}.")

                parent.update({ns.namer(k):v for k, v in value.items()})
            else:
                parent[ns.namer(name)] = value
        else:
            if not isinstance(nodes, list):
                raise ValueError(f"Aggregation function is marked to return node list but returns a single value.")
            if ns.be_merged:
                raise ValueError(f"Merging to parent dict requires folding.")

            parent[ns.namer(name)] = [self._serialize_node(n, ns) for n in nodes]

    def _find_serializer(self, prop: GraphTemplate.Property) -> list[Serializer]:
        return self.finder(prop.kind) if isinstance(prop.kind, type) else []

    def _serialize_node(self, node: Node, node_serializer: NodeSerializer) -> Any:
        # Cache values to generate NodeContext as NodeContextFactory for each node name.
        if not node.prop.name in self._context_factories:
            self._context_factories[node.prop.name] = NodeContextFactory(
                self,
                self._find_serializer(node.prop),
                self._node_params.get(node.prop.name, {}),
            )

        factory = self._context_factories[node.prop.name]

        with factory.begin(node, node_serializer._serializers) as cxt:
            value = cxt.serialize()

            # Child nodes are serialized only when the parent node is serialized into a dict.
            if isinstance(value, dict):
                for _, ch in node.children.items():
                    self.serialize_to(ch.prop.key, ch, value)

            return value


class SerializerMeta(NodeSerializing, type): # type: ignore
    @classmethod
    def __prepare__(cls, __name: str, __bases: tuple[type, ...], **kwds: Any) -> Mapping[str, object]:
        def wrap(n, f):
            def g(*args, **kwargs):
                ns = NodeSerializer()
                nf = getattr(ns, n)
                return nf(*args, **kwargs)
            return g
        return {n: wrap(n, f) for n, f in getmembers(NodeSerializing, isfunction) if not n.startswith("__")}


class S(metaclass=SerializerMeta):
    """
    An utility class to build `NodeSerializer` .

    This class provides class methods to create `NodeSerializer`
    each of which works in the same way as the method of the same name declared on `NodeSerializer` .

    Use them to supply `NodeSerializer`s to functions to serialize a graph or to create a graph schema
    such as `graph_dict` or `graph_schema` .

    ```python
    graph_dict(
        graph,
        a = S.of(),
        b = S.head(),
    )
    ```
    """
    @classmethod
    def of(
        cls,
        namer: str | Callable[[str], str] | None = None,
        aggregator: Callable[[list[Node]], Node] | Callable[[list[Node]], list[Node]] | None = None,
        *serializers: Serializer,
    ) -> 'NodeSerializer':
        """
        Create an instance of `NodeSerializer`.

        Args:
            namer: A string or naming function.
            aggregator: An aggregation function or an index of node to select in node container.
            serializer: A list of *serializer* s.
        Returns:
            Created `NodeSerializer` .
        """
        return NodeSerializer(namer, aggregator, *serializers)


def chain_serializers(serializers: list[Serializer]) -> Serializer:
    """
    Creates a serializer which chains given serializers.

    Args:
        serializers: A list of serializers.
    Returns:
        Chained serializer.
    """
    rt = inspect_serializers(serializers)
    def composed(cxt: NodeContext) -> rt: # type: ignore
        cxt_serializerrs = list(cxt._iterator) if cxt._iterator is not None else []
        cxt._iterator = iter(serializers[::-1] + cxt_serializerrs)
        return cxt.serialize()

    #composed.__annotations__['return'] = rt
    return composed


def inspect_serializers(serializers: list[Serializer]) -> type:
    """
    Inspects the return type of merged serializers.

    Args:
        serializers: A list of serializers.
    Returns:
        The return type of merged serializers.
    """
    # The last return type given explicitly is the return type of merged serializers.
    rt: type = Signature.empty
    for s in serializers[::-1]:
        t = signature(s).return_annotation
        if t != Signature.empty:
            try:
                # If the return annnotation is a generic type which takes a type parameter,
                # it is assumed that the actual type of the parameter is determined by the previous return type.
                t[_T]
                rt = t if rt == Signature.empty else rt[t] # type: ignore
            except TypeError:
                try:
                    return rt[t] # type: ignore
                except TypeError:
                    return t
    return rt
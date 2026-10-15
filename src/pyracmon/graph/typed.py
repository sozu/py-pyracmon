from collections.abc import Callable, Iterable, Iterator, MutableMapping
from typing import Any, dataclass_transform, get_type_hints, get_args, get_origin, overload, TYPE_CHECKING
from pyracmon.config import default_config
from pyracmon.graph.graph import Graph, NodeContainer, Node, ContainerView
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.typing import is_optional, issubgeneric
from pyracmon.graph.typing import is_optional, issubgeneric


@dataclass_transform()
class TNode[ENTITY]:
    """
    A view of a graph node that has an entity of type `ENTITY` and edges to child nodes.

    Subclasses can declare fields similar to those of a dataclass, each of which corresponds to an edge to child nodes.
    Although the nodes are always held in a node container in the underlying graph,
    an edge field can be declared without `TEdge` if a single node is expected as the child.

    ```python
    @dataclass
    class Post:
        title: str
        content: str

    @dataclass
    class User:
        name: str

    @dataclass
    class Image:
        url: str

    @dataclass
    class Comment:
        text: str

    class CommentNode(TNode[Comment]):
        user: User                       # A comment must be posted by a single user.

    class PostNode(TNode[Post]):
        author: User                     # A post must be written by a single user.
        comments: TEdge[CommentNode]     # A post can have multiple comments.
        images: TEdge[Image]             # A post can have multiple images.
        prev: PostNode | None            # A post can have a previous post.
        next: PostNode | None            # A post can have a next post.
        created: datetime                # A post has a created timestamp.

    class RecentPosts(TGraph):
        posts: TEdge[PostNode]
        total: int
    ```

    The code above is an example of a graph view for a graph that contains recent blog posts.
    An important point is that these declarations only provide type-safe access to the graph's structure; they do not change the graph's behavior.

    Args:
        ENTITY: The type of the entity held by the node.
    """
    def __init__(self, node: Node, graph: Graph):
        self.__node = node
        self.__graph = graph

    def __init_subclass__(cls) -> None:
        cls._fields_: dict[str, Callable[[TNode], Any]] = _parse_fields(cls)

        if base := next(filter(lambda b: get_origin(b) is TNode, getattr(cls, '__orig_bases__', ())), None):
            entity_type = get_args(base)[0]
        else:
            raise TypeError(f"{cls.__name__} must inherit from TNode directly with an entity type argument.")

        if not isinstance(entity_type, type):
            raise TypeError(f"{cls.__name__} must inherit from TNode with a concrete type.")

        cls._entity_type_: type = entity_type

    def __call__(self) -> ENTITY:
        """Returns the entity of the node."""
        return self.__node.entity

    def _get_node(self) -> Node:
        return self.__node

    def _get_graph(self) -> Graph:
        return self.__graph

    def _get_children(self, name: str) -> ContainerView:
        return {c.prop.key: c for c in self.__node.children.values()}[name].view

    if not TYPE_CHECKING:
        def __getattr__(self, name: str):
            """Returns a view of the child node with its declared type."""
            if conv := type(self)._fields_.get(name):
                return conv(self)
            else:
                raise AttributeError(f"{type(self).__name__} has no attribute {name!r}.")

        def __setattr__(self, name: str, value: Any):
            return _set_attribute(self, name, value)


class TEdge[ENTITY]:
    """
    Represents edges from a node to its child nodes.

    Args:
        ENTITY: The type of the entities of the child nodes.
    """
    def __init__(self, container: ContainerView[NodeContainer | Node.Children], graph: Graph, child_type: Any, to_view: bool):
        #: The container view of the child nodes.
        self.__container = container
        #: The type of the entities of the child nodes.
        self.__child_type = child_type
        #: Whether to return views of the child nodes or their entities.
        self.__to_view = to_view
        self.__graph = graph

    def __to_entity(self, node: Node) -> ENTITY:
        if self.__to_view:
            return self.__child_type(node, self.__graph) if node.entity is not None else None # type: ignore[return-value]
        else:
            return node.entity

    def __bool__(self) -> bool:
        """Returns whether this edge has any child nodes."""
        return bool(self.__container)

    def __call__(self) -> NodeContainer | Node.Children:
        """Returns the underlying container."""
        return self.__container()

    def __len__(self) -> int:
        """Returns the number of nodes."""
        return len(self.__container)

    def __iter__(self) -> Iterator[ENTITY]:
        """Iterates over the child nodes, yielding a value of type `ENTITY` for each."""
        for n in self.__container().nodes:
            yield self.__to_entity(n)

    @overload
    def __getitem__(self, index: int) -> ENTITY: ...
    @overload
    def __getitem__(self, index: slice) -> Iterable[ENTITY]: ...
    def __getitem__(self, index: int | slice) -> ENTITY | Iterable[ENTITY]:
        """Returns the value of type `ENTITY` for the child node at the given index, or a list of such values if `index` is a slice."""
        if isinstance(index, slice):
            return [self.__to_entity(n) for n in self.__container().nodes[index]]
        else:
            node = self.__container().nodes[index]
            return self.__to_entity(node)

    def find(self, pred: Callable[[ENTITY], bool]) -> ENTITY | None:
        """
        Finds the first child node that satisfies the given predicate.

        Args:
            pred: A predicate function that takes a value of type `ENTITY` and returns a boolean.
        Returns:
            The value of type `ENTITY` for the first child node that satisfies the predicate, or `None` if no such node exists.
        """
        return next(filter(pred, self), None)

    def append(self, value: ENTITY) -> None:
        """
        Appends a new child node with the given entity.

        Args:
            value: The entity of the new child node.
        """
        cc = self.__container()

        ancestors: MutableMapping[str, list[Node]] = {}
        if isinstance(cc, Node.Children):
            if pp := cc.prop.parent:
                for n in self.__graph.containers[pp.name].nodes:
                    if any(cc is c for c in n.children.values()):
                        ancestors[pp.name] = [n]
                        break

        if isinstance(value, TNode):
            _append_node(self.__graph, cc.prop, value._get_node(), ancestors)
        elif isinstance(cc, NodeContainer):
            cc.append(value, {})
        else:
            container = self.__graph.containers[cc.prop.name]
            container.append(value, ancestors)

    def extend(self, values: Iterable[ENTITY]) -> None:
        """
        Appends multiple new child nodes with the given entities.

        Args:
            values: An iterable of entities for the new child nodes.
        """
        for v in values:
            self.append(v)


@dataclass_transform()
class TGraph:
    """
    The base class for graph views that provide type-safe access to a graph's structure.

    If subclass is declared with `nested=True`, the keys of descendant nodes are prefixed with the names of their parent nodes, separated by dots.
    This feature allows for the declaration of nested graph views, where child nodes can have attributes of the same names as other nodes in the graph.
    """
    def __init__(self, graph: Graph):
        self.__graph = graph

    def __init_subclass__(cls, nested: bool = False) -> None:
        cls._fields_: dict[str, Callable[[TGraph], Any]] = _parse_fields(cls)
        props, rels = _to_properties(cls, [], nested=nested)
        cls._nested = nested
        cls._properties_: dict[str, type] = props
        cls._relations_: list[tuple[str, str]] = rels

    @classmethod
    def template(cls, spec: GraphSpec | None = None) -> GraphTemplate:
        """
        Returns a graph template for this graph view class.

        Args:
            spec: A graph specification that defines how to serialize model objects. If not given, the default graph specification is used.
        Returns:
            A graph template for this graph view class.
        """
        return _to_template(cls._nested, spec or default_config().graph_spec, cls._properties_, cls._relations_)

    def __call__(self) -> Graph:
        """Returns the graph instance."""
        return self.__graph

    def __iter__(self) -> Iterator[tuple[str, ContainerView]]:
        """Iterates over the containers in the graph."""
        return map(lambda c: (c.name, c.view), filter(lambda c: c.prop.parent is None, self.__graph.containers.values()))

    def _get_graph(self) -> Graph:
        return self.__graph

    def _get_children(self, name: str) -> ContainerView:
        return self.__graph.containers[name].view

    if not TYPE_CHECKING:
        def __getattr__(self, name: str):
            """
            Returns a view of the named property with its declared type.

            NOTE:
                Because this class inherits from `GraphView`,
                this method must be implemented but it ends up allowing arbitrary attribute access in type checking.
            """
            if conv := type(self)._fields_.get(name):
                return conv(self)
            else:
                raise AttributeError(f"{type(self).__name__} has no attribute {name!r}.")

        def __setattr__(self, name: str, value: Any):
            return _set_attribute(self, name, value)


class TypedGraph[GRAPH: TGraph](Graph):
    """
    A `Graph` subclass that provides a `TGraph` instance as a view of itself.

    Args:
        GRAPH: The type of the graph view.
    """
    def __init__(self, typed: type[GRAPH], template: GraphTemplate):
        super().__init__(template)
        self._typed = typed

    @property
    def view(self) -> GRAPH:
        if self._view is None:
            self._view = self._typed(self)
        return self._view


def new_typed_graph[GRAPH: TGraph](cls: type[GRAPH], spec: GraphSpec | None = None) -> TypedGraph[GRAPH]:
    """
    Creates a new `TypedGraph` with the schema defined by the given graph view class.

    Args:
        cls: A graph view class that defines the schema of the graph.
        spec: A graph specification that defines how to serialize model objects. If not given, the default graph specification is used.
    Returns:
        The created `TypedGraph` instance.
    """
    return TypedGraph(cls, _to_template(cls._nested, spec or default_config().graph_spec, cls._properties_, cls._relations_))


def _dump(value: Any):
    if isinstance(value, TEdge):
        return [_dump(v) for v in value]
    elif isinstance(value, TNode):
        return (value(), dump_typed_graph(value))
    else:
        return value


def dump_typed_graph(graph: TGraph | TNode) -> dict[str, Any]:
    values: dict[str, Any] = {}
    gt = type(graph)
    for f in gt._fields_.keys():
        v = getattr(graph, f)
        values[f] = _dump(v)
    return values


def _to_properties(cls: type[TGraph | TNode], path: list[str], nested: bool = True) -> tuple[dict[str, type], list[tuple[str, str]]]:
    """
    Parse the fields declared in a `TGraph` / `TNode` into properties and relations.

    This function walks the given class and its child nodes recursively,
    and returns a mapping of property names to their types, along with a list of relations between properties.
    """
    props: dict[str, type] = {}
    rels: list[tuple[str, str]] = []

    def put(n: str, t: type):
        if n in props:
            raise TypeError(f"Duplicate property name {n!r} found in {cls.__name__} and its child nodes.")
        props[n] = t

    for name, ann in get_type_hints(cls, include_extras=False).items():
        # TEdge[prop_type] -> prop_type
        is_edge = get_origin(ann) is TEdge
        prop_type = get_args(ann)[0] if is_edge else ann
        if opt := is_optional(prop_type):
            prop_type = opt

        new_path = path + [name]
        name = ".".join(new_path) if nested else name

        if issubgeneric(prop_type, TNode):
            # prop_type: TNode[entity_type] -> entity_type
            node_type: type[TNode] = prop_type

            put(name, node_type._entity_type_)

            nps, nrs = _to_properties(node_type, new_path, nested)

            for n, t in nps.items():
                put(n, t)

            # Add relation between this property and child properties of the TNode.
            field_names = {".".join(new_path + [f]) if nested else f for f in node_type._fields_.keys()}
            rels.extend([(name, n) for n in nps.keys() if n in field_names])

            # Add properties and relations of the child node recursively.
            rels.extend(nrs)
        else:
            put(name, prop_type)

    return props, rels


def _parse_fields[TMPL: TGraph | TNode](cls: type[TMPL]) -> dict[str, Callable[[TMPL], Any]]:
    """
    Parse the fields declared in a `TGraph` or `TNode` subclass, and return a mapping from field names to functions that convert a graph view into the field's value.

    Unlike an untyped graph view, fields that are not declared as `TEdge` return a single value of the declared type.
    """
    fields: dict[str, Callable[[TMPL], Any]] = {}

    hints = get_type_hints(cls, include_extras=False)

    for name, ann in hints.items():
        if get_origin(ann) is TEdge:
            child_type = get_args(ann)[0]

            if opt := is_optional(child_type):
                child_type = opt

            to_view = issubgeneric(child_type, TNode)
            fields[name] = edger(cls, name, child_type, to_view)
        else:
            opt = is_optional(ann)
            field_type = opt if opt else ann

            if issubgeneric(field_type, TNode):
                fields[name] = noder(cls, name, field_type, opt is not None)
            else:
                fields[name] = valuer(cls, name, field_type, opt is not None)

    return fields


def _to_template(nested: bool, spec: GraphSpec, props: dict[str, type], rels: list[tuple[str, str]]) -> GraphTemplate:
    # Convert declared fields into pairs of name and simple type which are available for graph template properties.
    def conv(t: Any) -> Any:
        if ot := is_optional(t):
            return ot
        else:
            return t
    props = {n: conv(t) for n, t in props.items()}
    tmpl = spec.new_template(nested, **props)

    for pn, cn in rels:
        parent = tmpl._properties[pn]
        child = tmpl._properties[cn]
        _ = parent << child

    return tmpl


def _switch_property(base: GraphTemplate.Property, prop: GraphTemplate.Property) -> str:
    if base._nested ^ prop._nested:
        raise ValueError("Nested states of nodes are incompatible.")

    if base._nested:
        return '.'.join([base.name, prop.key])
    else:
        return prop.name


def _append_node(graph: Graph, prop: GraphTemplate.Property, node: Node, ancestors: MutableMapping[str, list[Node]]) -> None:
    if prop._nested ^ node.prop._nested:
        raise ValueError("Container and node nested states are incompatible.")

    container = graph.containers[prop.name]
    container.append(node.entity, ancestors)

    for c in node.children.values():
        pn = _switch_property(prop, c.prop)
        p = graph.template._properties[pn]
        for n in c.nodes:
            _append_node(graph, p, n, dict(**ancestors))


def _set_attribute(obj: TGraph | TNode, name: str, value: Any) -> None:
    if name not in type(obj)._fields_:
        return object.__setattr__(obj, name, value)

    graph = obj._get_graph()

    # Remove target node if given, otherwise remove all nodes in the container.
    nodes: list[Node]
    prop: GraphTemplate.Property
    ancestors: dict[str, list[Node]] = {}

    if isinstance(obj, TNode):
        n = obj._get_node()
        nodes = []
        for c in n.children.values():
            if c.prop.key == name:
                prop = c.prop
                nodes.extend(c.nodes)
                break
        ancestors[n.prop.name] = [n]
    else:
        container = graph.containers[name]
        nodes = list(container.nodes)
        prop = container.prop

    for n in nodes:
        graph.remove(n)

    if isinstance(value, TNode):
        _append_node(graph, prop, value._get_node(), ancestors)
    elif isinstance(value, TEdge):
        for n in value().nodes:
            _append_node(graph, prop, n, dict(**ancestors))
    else:
        graph.containers[prop.name].append(value, ancestors)


class edger[TMPL: TGraph | TNode]:
    def __init__(self, owner_type: type, name: str, child_type: Any, to_view: bool):
        self.owner_type = owner_type
        self.name = name
        self.child_type = child_type
        self.to_view = to_view

    def __eq__(self, value: object) -> bool:
        return (isinstance(value, edger)
            and self.owner_type == value.owner_type
            and self.name == value.name
            and self.child_type == value.child_type
            and self.to_view == value.to_view
        )

    def __call__(self, s: TMPL) -> TEdge[Any]:
        # TODO cache can be used for any successive calls.
        return TEdge(s._get_children(self.name), s._get_graph(), self.child_type, self.to_view)


class noder[TMPL: TGraph | TNode]:
    def __init__(self, owner_type: type, name: str, node_type: Any, is_opt: bool):
        self.owner_type = owner_type
        self.name = name
        self.node_type = node_type
        self.is_opt = is_opt

    def __eq__(self, value: object) -> bool:
        return (
            isinstance(value, noder)
            and self.owner_type == value.owner_type
            and self.name == value.name
            and self.node_type == value.node_type
            and self.is_opt == value.is_opt
        )

    def __call__(self, s: TMPL) -> Any:
        # TODO use cache when the identical node is found.
        children = s._get_children(self.name)

        node: Node | None = children[0](...) if children else None
        entity = node.entity if node else None

        if entity is None:
            if self.is_opt:
                return None
            else:
                raise AttributeError(f"Attribute {self.name!r} of {self.owner_type.__name__} is not set.")
        else:
            return self.node_type(node, s._get_graph())


class valuer[TMPL: TGraph | TNode]:
    def __init__(self, owner_type: type, name: str, entity_type: Any, is_opt: bool):
        self.owner_type = owner_type
        self.name = name
        self.entity_type = entity_type
        self.is_opt = is_opt

    def __eq__(self, value: object) -> bool:
        return (
            isinstance(value, valuer)
            and self.owner_type == value.owner_type
            and self.name == value.name
            and self.entity_type == value.entity_type
            and self.is_opt == value.is_opt
        )

    def __call__(self, s: TMPL) -> Any:
        children = s._get_children(self.name)
        if self.is_opt:
            return children[0]() if children else None
        elif not children:
            raise AttributeError(f"Attribute {self.name!r} of {self.owner_type.__name__} is not set.")
        else:
            return children[0]()
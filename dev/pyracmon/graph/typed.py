# This module provides type safe access to graphs.
# Currently, it is an experimental implementation which has some constraints.
# - All fields to TNode subclasses must have unique names in a TGraph.
#
# This feature is planned to be merged into core graph module in the future version.
# Therefore, any object exposed in this module will be deprecated and be available by other names and ways.
from collections.abc import Callable, Iterable, Iterator
from typing import Any, dataclass_transform, get_type_hints, get_args, get_origin, overload
from pyracmon.config import default_config
from pyracmon.graph.graph import Graph, Node, ContainerView
from pyracmon.graph.spec import GraphSpec
from pyracmon.graph.template import GraphTemplate
from pyracmon.graph.typing import is_optional, issubgeneric
from pyracmon.graph.typing import is_optional, issubgeneric


@dataclass_transform()
class TNode[ENTITY]:
    """
    View of a graph node which has an entity of type `ENTITY` and edges to child nodes.

    Subclasses can declare fields similar to dataclass fields, each of which corresponds to edges to child nodes.
    Although the nodes must be contained in a node container in the actual graph instance,
    the edge fields can be declared without specifying `TEdge` if a single node is expected as a child.

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

    The above code is an example of a graph view which contains recent blog posts.
    An important point is that the declarations just provide type safe access to the graph structure, and do not change the behavior of the graph.

    Args:
        ENTITY: Type of entity of the node.
    """
    def __init__(self, node: Node):
        self.__node = node

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
        """Return an entity of the node."""
        return self.__node.entity

    def _get_children(self, name: str) -> ContainerView:
        return self.__node.children[name].view

    def __getattr__(self, name: str):
        """Returns a view of child node along with declared type."""
        if conv := type(self)._fields_.get(name):
            return conv(self)
        else:
            raise AttributeError(f"{type(self).__name__} has no attribute {name!r}.")


class TEdge[ENTITY]:
    """
    Represents edges from a node to its child nodes.
    """
    def __init__(self, container: ContainerView[Node.Children], child_type: Any, to_view: bool):
        #: Container view of child nodes.
        self.__container = container
        #: Type of entities of child nodes.
        self.__child_type = child_type
        #: Whether to return views of child nodes or their entities.
        self.__to_view = to_view

    def __to_entity(self, node: Node) -> ENTITY:
        if self.__to_view:
            return self.__child_type(node) if node.entity is not None else None # type: ignore[return-value]
        else:
            return node.entity

    def __bool__(self) -> bool:
        """Returns whether this edge has any child nodes."""
        return bool(self.__container)

    def __call__(self) -> Node.Children:
        """Returns a base container."""
        return self.__container()

    def __len__(self) -> int:
        """Returns the number of nodes."""
        return len(self.__container)

    def __iter__(self) -> Iterator[ENTITY]:
        """Iterates views of nodes."""
        for n in self.__container().nodes:
            yield self.__to_entity(n)

    @overload
    def __getitem__(self, index: int) -> ENTITY: ...
    @overload
    def __getitem__(self, index: slice) -> Iterable[ENTITY]: ...
    def __getitem__(self, index: int | slice) -> ENTITY | Iterable[ENTITY]:
        """Returns a view of a node at the index."""
        if isinstance(index, slice):
            return [self.__to_entity(n) for n in self.__container().nodes[index]]
        else:
            node = self.__container().nodes[index]
            return self.__to_entity(node)


@dataclass_transform()
class TGraph:
    """
    View of a graph whose schema is defined by the subclass of this class.
    """
    def __init__(self, graph: Graph):
        self.__graph = graph

    def __init_subclass__(cls) -> None:
        cls._fields_: dict[str, Callable[[TGraph], Any]] = _parse_fields(cls)
        props, rels = _to_properties(cls)
        cls._properties_: dict[str, type] = props
        cls._relations_: list[tuple[str, str]] = rels

    def __call__(self) -> Graph:
        """Returns the graph instance."""
        return self.__graph

    def _get_children(self, name: str) -> ContainerView:
        return self.__graph.containers[name].view

    def __getattr__(self, name: str):
        """Returns a view of an attribute along with declared type."""
        if conv := type(self)._fields_.get(name):
            return conv(self)
        else:
            raise AttributeError(f"{type(self).__name__} has no attribute {name!r}.")


class TypedGraph[GRAPH: TGraph](Graph):
    """
    Graph class which provides `TGraph` as a view of itself.

    Args:
        GRAPH: Type of graph view.
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
    Create a new `TypedGraph` with the schema defined by the given graph view class.

    Args:
        cls: A graph view class which defines the schema of the graph.
        spec: A graph specification which defines how to serialize model objects. If not given, the default graph specification is used.
    Returns:
        Created `TypedGraph` instance.
    """
    # Convert declared fields into pairs of name and simple type which are available for graph template properties.
    def conv(t: Any) -> Any:
        if ot := is_optional(t):
            return ot
        else:
            return t
    props = {n: conv(t) for n, t in cls._properties_.items()}
    tmpl = (spec or default_config().graph_spec).new_template(**props)

    for pn, cn in cls._relations_:
        parent = tmpl._properties[pn]
        child = tmpl._properties[cn]
        _ = parent << child

    return TypedGraph(cls, tmpl)


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


def _to_properties(cls: type[TGraph | TNode]) -> tuple[dict[str, type], list[tuple[str, str]]]:
    """
    Parse fields declared in a `TGraph` / `TNode` into properties and relations.

    This function walks given class and its child nodes recursively,
    and returns a mapping from property names to their types and a list of relations between properties.
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

        if issubgeneric(prop_type, TNode):
            # prop_type: TNode[entity_type] -> entity_type
            node_type: type[TNode] = prop_type

            put(name, node_type._entity_type_)

            nps, nrs = _to_properties(node_type)

            for n, t in nps.items():
                put(n, t)

            # Add relation between this property and child properties of the TNode.
            rels.extend([(name, n) for n in nps.keys() if n in node_type._fields_])

            # Add properties and relations of the child node recursively.
            rels.extend(nrs)
        else:
            put(name, prop_type)

    return props, rels


def _parse_fields[TMPL: TGraph | TNode](cls: type[TMPL]) -> dict[str, Callable[[TMPL], Any]]:
    """
    Parse fields declared in a TGraph or TNode subclass and return a mapping from field names to functions which convert a graph view into the field value.

    Unlike untyped graph view, fields which are not declared as `TEdge` return a single value of the declared type.
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
        return TEdge(s._get_children(self.name), self.child_type, self.to_view)


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
        children = s._get_children(self.name)

        node: Node | None = children[0](...) if children else None
        entity = node.entity if node else None

        if entity is None:
            if self.is_opt:
                return None
            else:
                raise AttributeError(f"Attribute {self.name!r} of {self.owner_type.__name__} is not set.")
        else:
            return self.node_type(node)


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
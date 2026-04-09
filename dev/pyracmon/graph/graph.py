"""
This module exports types representing graphs.
"""
from typing import TypeVar, Generic, Protocol, Union, Optional, Any, overload, cast
from typing_extensions import Self
from collections.abc import MutableMapping, Iterable, Iterator
from typing import Any
from .identify import IdentifyPolicy, neverPolicy
from .template import GraphTemplate
from .protocol import *


T = TypeVar('T', covariant=True)


class GraphView(Protocol):
    """
    The interface of the view of graph.
    """
    def __call__(self) -> 'Graph': ...
    def __iter__(self) -> Iterator[tuple[str, 'ContainerView[NodeContainer]']]:
        """
        Iterates root container views.

        Returns:
            Iterator of pairs of name and container view.
        """
        ...
    def __getattr__(self, name: str) -> 'ContainerView':
        """
        Returns a container view by its name.

        Args:
            name: Container name. i.e. template property name for the node container.
        Returns:
            Container view.
        """
        ...


class Graph:
    """
    This class represents a graph composed of tree-structured node containers.

    The structure is determined by `GraphTemplate`. Use `new_graph` Instead of constructor to create new graph instance.

    ```python
    template = GraphSpac().new_template(
        a = (int, lambda x:x),
        b = (str, lambda x:x),
        c = (str, lambda x:x),
    )
    template.a << template.b << template.c
    graph = new_graph(template)
    ```

    In above code, a graph which has 3 properties ( `a` `b` `c` ) and a structure where `a` is parent of `b` and `b` is parent of `c` is created.

    `append` ( `replace` ) is a method to store entities in the graph with tying them each other according to the structure.
    Entites are encapsulated by `Node` which can have an edge to parent node.

    ```python
    graph.append(a=1, b="a", c="x").append(a=2, b="b", c="y")
    ```

    In `append`, entities are first sorted in descending order, and then:

    - Search a node whose entity is *identical* to the first entity from the corresponding node container.
        - If found, new node is not created and the *identical* node is set to next parent.
        - Otherwise, new node is appended and it is set to next parent.
    - Apply this to following entities in order. A difference is that *identical* node is searched from the sequence of parents in the session.

    In example here, the identification is done by entity value itself ( `lambda x:x` ). Next code is the example where *identical* nodes are found.

    ```python
    graph.append(a=1, b="a", c="z").append(a=2, b="c", c="y")
    ```

    In the first `append`, `a` and `b` has its *identical* node and `a` is *identical* in the second.
    `c` in the second one is not *identical* to any node because parent node `b="c"` is already added as new node.

    Due to the identification mechanism, entity relationships in the graph is guaranteed after repeating `append` .
    """
    def __init__(self, template: GraphTemplate):
        #: Graph template.
        self.template: GraphTemplate = template
        #: A `dict` containing node containers by their names.
        self.containers: dict[str, NodeContainer] = {p.name:self._to_container(p) for p in template}
        self._view = None

    def _to_container(self, prop: GraphTemplate.Property) -> 'NodeContainer':
        if isinstance(prop.kind, GraphTemplate):
            return _GraphNodeContainer(prop)
        else:
            return NodeContainer(prop)

    def _container_of(self, prop: GraphTemplate.Property) -> Optional['NodeContainer']:
        candidates = [c for c in self.containers.values() if c.prop.is_compatible(prop)]
        if len(candidates) > 1:
            raise ValueError(f"Container can't be determined from property '{prop.name}'.")
        return candidates[0] if candidates else None

    def __add__(self, another: Union[Self, GraphView]) -> 'Graph':
        """
        Create new graph by adding this graph and another graph.

        New graph has the same template as this graph's.
        On the other hand, because this method depends on `__iadd__()`, another graph must not have the same template.

        Args:
            another: Graph or its view.
        Returns:
            Created graph.
        """
        graph = Graph(self.template)

        graph += self
        graph += another

        return graph

    def __iadd__(self, another: Union[Self, GraphView]) -> Self:
        """
        Append nodes from another graph.

        Nodes of another graph are traversed from its root and appended to compatible containers each other.

        Args:
            another: Graph or its view.
        Returns:
            This graph.
        """
        graph = another if isinstance(another, Graph) else another()

        def add(n: Node, anc: dict[str, list[Node]]):
            c = self._container_of(n.prop)
            if c:
                c.append(n.entity, anc)
            for ch_ in n.children.values():
                for m in ch_.nodes:
                    add(m, anc.copy())

        for c_ in graph.roots:
            for n_ in c_.nodes:
                add(n_, {})

        return self

    @property
    def roots(self) -> Iterable['NodeContainer']:
        """
        Returns root node containers.
        """
        return filter(lambda c: c.prop.parent is None, self.containers.values())

    @property
    def view(self) -> GraphView:
        """
        Returns an unmodifiable view of this graph.

        The view object works as the accessor to graph nodes.

        ```python
        >>> template = GraphSpac().new_template(a=int, b=str, c=str)
        >>> template.a << template.b
        >>> graph = new_graph(template)
        >>> view = graph.view
        >>> assert view() is graph                        # invocation
        >>> assert view.a is graph.containers["a"].view   # attribute
        >>> assert [c().name for c in view] == ["a", "c"] # iteration
        ```
        """
        if self._view is None:
            graph = self
            class _GraphView:
                def __call__(self) -> Graph:
                    """Returns the greph of this view."""
                    return graph
                def __iter__(self) -> Iterator[tuple[str, ContainerView[NodeContainer]]]:
                    """Iterates views of root containers."""
                    return map(lambda c: (c.name, c.view), filter(lambda c: c.prop.parent is None, graph.containers.values()))
                def __getattr__(self, name: str) -> ContainerView:
                    """Returns a view of a container of the name."""
                    return graph.containers[name].view
            self._view = _GraphView()
        return self._view

    def _append(self, to_replace: bool, entities: dict[str, Any]) -> Self:
        props = [p for p in self.template if p.name in entities]

        filtered = set()
        for p in props:
            if (p.parent is None) or (p.parent.name not in entities) or (p.parent.name in filtered):
                if p.entity_filter is None or p.entity_filter(entities[p.name]):
                    filtered.add(p.name)

        ancestors = {}
        for k in [p.name for p in props if p.name in filtered]:
            self.containers[k].append(entities[k], ancestors, to_replace)

        return self

    def append(self, **entities: Any) -> Self:
        """
        Append entities with associated property names.

        Args:
            entities: Entities keyed with associated property names.
        Returns:
            This graph.
        """
        return self._append(False, entities)

    def replace(self, **entities: Any) -> Self:
        """
        Works similarly to `append`, but entities of identical nodes are replaced with given entities.

        Args:
            entities: Entities keyed with associated property names.
        Returns:
            This graph.
        """
        return self._append(True, entities)


def new_graph(template: GraphTemplate, *bases: Union[Graph, GraphView]) -> Graph:
    """
    Create a graph from a template.

    Use this function instead of invoking constructor directly.

    Args:
        template: A template of a graph.
        bases: Other graphs whose nodes are appended to created graph.
    Returns:
        Created graph.
    """
    graph = Graph(template)

    for b in bases:
        graph += b

    return graph


class ContainerView(Protocol, Generic[T]):
    """
    The interface of the view of a node set, i.e. `NodeContainer` and `Node.Children` .
    """
    def __bool__(self) -> bool:
        """Returns whether this container is not empty."""
        ...
    def __call__(self) -> T:
        """Returns a base container."""
        ...
    def __len__(self) -> int:
        """Returns the number of nodes."""
        ...
    def __iter__(self) -> Iterator['NodeView']:
        """Iterates views of nodes."""
        ...
    @overload
    def __getitem__(self, index: int) -> 'NodeView': ...
    @overload
    def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
    def __getitem__(self, index: Union[int, slice]) -> Union['NodeView', Iterable['NodeView']]:
        """Returns a view of a node at the index."""
        ...
    def __getattr__(self, key) -> 'ContainerView':
        """Returns a view of the first node or empty container view if it does not exist."""
        ...


class _EmptyContainerView(ContainerView[None]):
    def __init__(self, prop):
        self.prop = prop

    def __bool__(self):
        return False

    def __call__(self) -> None:
        return None

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0

    def __getitem__(self, index):
        if isinstance(index, slice):
            return []
        else:
            raise IndexError(f"Index for container '{self.prop.name}' is out of range.")

    def __getattr__(self, key):
        child = next(filter(lambda c: c.name == key, self.prop.children), None)
        if child:
            return _EmptyContainerView(child)
        else:
            raise KeyError(f"Graph property '{self.prop.name}' does not have a child property '{key}'.")


class NodeContainer:
    """
    This class represents a container of nodes for a template property.
    """
    def __init__(self, prop: GraphTemplate.Property):
        #: Template property.
        self.prop = prop
        self.nodes: list[Node] = []
        self.keys: dict[Any, list[int]] = {}
        self._view = None

    @property
    def name(self) -> str:
        """
        Returns the container name, which is same as the name of template property.
        """
        return self.prop.name

    @property
    def view(self) -> ContainerView['NodeContainer']:
        """
        Returns an unmodifiable view of this container.

        The view object works as the accessor to container components.

        ```python
        template = GraphSpac().new_template(a=int, b=str, c=str)
        template.a << template.b
        graph = new_graph(template).append(a=1, b="a").append(a=1, b="b").append(a=2, b="c")
        container = graph.containers["a"]
        view = graph.view.a
        assert view() is container                             # invocation
        assert view.b is container.nodes[0].children["b"].view # attribute
        assert view[1] is container.nodes[1].view              # index
        assert [n() for n in view] == [1, 2]                   # iteration
        assert len(view) == 2                                  # length
        ```
        """
        if self._view is None:
            container = self
            class _ContainerView:
                def __bool__(self):
                    """Returns whether this container is not empty."""
                    return len(container.nodes) != 0
                def __call__(self) -> NodeContainer:
                    """Returns a base container."""
                    return container
                def __len__(self):
                    """Returns the number of nodes."""
                    return len(container.nodes)
                def __iter__(self):
                    """Iterates views of nodes."""
                    return map(lambda n: n.view, container.nodes)
                @overload
                def __getitem__(self, index: int) -> 'NodeView': ...
                @overload
                def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
                def __getitem__(self, index: Union[int, slice]) -> Union['NodeView', Iterable['NodeView']]:
                    """Returns a view of a node at the index."""
                    if isinstance(index, slice):
                        return [n.view for n in container.nodes[index]]
                    else:
                        return container.nodes[index].view
                def __getattr__(self, key) -> ContainerView:
                    """Returns a view of the first node or empty container view if it does not exist."""
                    child = next(filter(lambda c: c.name == key, container.prop.children), None)
                    if child:
                        return container.nodes[0].children[key].view if len(container.nodes) > 0 else _EmptyContainerView(child)
                    else:
                        raise KeyError(f"Graph property '{container.prop.name}' does not have a child property '{key}'.")
            self._view = _ContainerView()
        return self._view

    def append(self, entity: Any, ancestors: MutableMapping[str, list['Node']], to_replace: bool = False):
        """
        Add an entity to this container.

        Identical node is searched by examining whether this container already contains a node of the identical entity
        and its parent is found in `anscestors` .

        Args:
            entity: An entity to be stored in the node.
            ancestors: Parent nodes mapped by property names.
            to_replace: If `True`, the entity of identical node is replaced. Otherwise, it is not changed.
        """
        policy: IdentifyPolicy = self.prop.policy or neverPolicy()

        key = policy.get_identifier(entity)

        parents, identicals = policy.identify(self.prop, [self.nodes[i] for i in self.keys.get(key, [])], ancestors)

        new_nodes = identicals.copy()

        for pn in parents:
            index = len(self.nodes)

            node = Node(self.prop, entity, key, index)
            self.nodes.append(node)
            if key is not None:
                self.keys.setdefault(key, []).append(index)
            new_nodes.append(node)

            if pn is not None:
                pn.add_child(node)

        if to_replace:
            for n in identicals:
                n.entity = entity

        ancestors[self.prop.name] = new_nodes


class _GraphNodeContainer(NodeContainer):
    """
    NodeContainer which contains graph instances.
    """
    def append(self, entity: Any, ancestors: MutableMapping[str, Iterable['Node']], to_replace: bool = False):
        if not isinstance(entity, (dict, Graph)):
            raise ValueError(f"Node of graph only accepts dict or Graph object.")

        policy = self.prop.policy or neverPolicy()

        parents, _ = policy.identify(self.prop, cast(list[Node], []), ancestors)

        for pn in parents:
            index = len(self.nodes)

            graphs = []

            if pn is None or len(pn.children[self.name].nodes) == 0:
                g = Graph(cast(GraphTemplate, self.prop.kind))
                node = _GraphNode(self.prop, g, None, index)
                self.nodes.append(node)

                if pn is not None:
                    pn.add_child(node)

                graphs.append(g)
            else:
                graphs.extend([n.entity for n in pn.children[self.name].nodes])

            for g in graphs:
                if isinstance(entity, dict):
                    g.append(**entity)
                else:
                    g += entity


class NodeView:
    def __call__(self, alt: Any = None) -> Any:
        """Returns an entity of this node."""
        ...
    def __getattr__(self, key: str) -> ContainerView:
        """Returns a view of child nodes by its name."""
        ...
    def __iter__(self) -> Iterator[tuple[str, ContainerView]]:
        """Iterate key-value pairs of child nodes."""
        ...


class Node:
    """
    This class represents a node which contains an entity.
    """
    class Children:
        """
        This class represents a child nodes of a node.
        """
        def __init__(self, prop: GraphTemplate.Property):
            #: Template property.
            self.prop = prop
            self.nodes: list[Node] = []
            self.keys = set()
            self._view = None

        @property
        def name(self) -> str:
            """
            Returns the name of corresponding template property.
            """
            return self.prop.name

        @property
        def view(self) -> ContainerView['Node.Children']:
            """
            Returns an unmodifiable view of child nodes.
            """
            if self._view is None:
                base = self
                class _ChildrenView:
                    def __bool__(self):
                        """Returns whether this container is not empty."""
                        return len(base.nodes) != 0
                    def __call__(self):
                        """Returns children container."""
                        return base
                    def __iter__(self):
                        """Iterates views of child nodes."""
                        return map(lambda n: n.view, base.nodes)
                    def __len__(self):
                        """Returns the number of child nodes."""
                        return len(base.nodes)
                    @overload
                    def __getitem__(self, index: int) -> 'NodeView': ...
                    @overload
                    def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
                    def __getitem__(self, index):
                        """Returns a view of child node at the index."""
                        if isinstance(index, slice):
                            return [n.view for n in base.nodes[index]]
                        else:
                            return base.nodes[index].view
                    def __getattr__(self, key):
                        """Returns a view of the first node or empty container view if it does not exist."""
                        child = next(filter(lambda c: c.name == key, base.prop.children), None)
                        if child:
                            return base.nodes[0].children[key].view if len(base.nodes) > 0 else _EmptyContainerView(child)
                        else:
                            raise KeyError(f"Graph property '{base.prop.name}' does not have a child property '{key}'.")
                self._view = _ChildrenView()
            return self._view

        def __contains__(self, node: 'Node') -> bool:
            return node in self.keys

        def __iter__(self) -> Iterator['Node']:
            return iter(self.nodes)

        def append(self, node):
            if node not in self.keys:
                self.keys.add(node)
                self.nodes.append(node)

    def __init__(self, prop: GraphTemplate.Property, entity: Any, key: Optional[Any], index: int):
        #: Template property.
        self.prop = prop
        #: An entity value.
        self.entity = entity
        self.key = key
        self.parents = set()
        self.children: dict[str, Node.Children] = {c.name: Node.Children(c) for c in prop.children}
        self._index = index
        self._view = None

    def __contains__(self, key: str) -> bool:
        return key in self.children

    @property
    def name(self) -> str:
        """
        Returns the container name, which is same as the name of template property.
        """
        return self.prop.name

    @property
    def view(self) -> NodeView:
        """
        Returns an unmodifiable view of this node.

        The view object works as the accessor to entity and child nodes.
        """
        if self._view is None:
            node = self
            class _NodeView(NodeView):
                def __call__(self, alt: Any = None) -> Any:
                    """Returns an entity of this node."""
                    return node.entity
                def __getattr__(self, key: str) -> ContainerView:
                    """Returns a view of child nodes by its name."""
                    return node.children[key].view
                def __iter__(self) -> Iterator[tuple[str, ContainerView]]:
                    """Iterate key-value pairs of child nodes."""
                    return map(lambda nc: (nc[0], nc[1].view), node.children.items())
            self._view = _NodeView()
        return self._view

    def add_child(self, child: 'Node') -> Self:
        """
        Adds a child node.

        Args:
            child: Child node.
        Returns:
            This instance.
        """
        if child.prop.template != self.prop.template:
            raise ValueError(f"Nodes from different graph template can't be associated.")
        self.children[child.prop.name].append(child)
        child.parents.add(self)
        return self

    def has_child(self, child: 'Node') -> bool:
        """
        Checks this node contains the node identical to given node.

        Args:
            child: Node to search.
        Returns:
            `True` if exists.
        """
        if child.prop.template != self.prop.template:
            return False
        elif child.prop.name in self.children:
            return child in self.children[child.prop.name].keys
        else:
            return False


class _GraphNode(Node):
    @property
    def view(self):
        return self.entity.view

    def add_child(self, child):
        raise TypeError(f"GraphNode does not have child.")

    def has_child(self, child):
        return False
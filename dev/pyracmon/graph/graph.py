"""
This module exports types representing graphs.
"""
from typing import Protocol, Any, overload, cast
from typing_extensions import Self
from collections.abc import MutableMapping, Iterable, Iterator
from typing import Any
from .identify import IdentifyPolicy, neverPolicy
from .template import GraphTemplate


class GraphView(Protocol):
    """
    The interface of the view of a graph.
    """
    def __call__(self) -> 'Graph': ...
    def __iter__(self) -> Iterator[tuple[str, 'ContainerView[NodeContainer]']]:
        """
        Iterates root container views.

        Returns:
            An iterator of pairs of a container name and its view.
        """
        ...
    def __getattr__(self, name: str) -> 'ContainerView':
        """
        Returns a container view by its name.

        Args:
            name: The container name, i.e., the template property name of the node container.
        Returns:
            The view of the container.
        """
        ...


class Graph:
    """
    This class represents a graph composed of tree-structured node containers.

    The structure is determined by `GraphTemplate`. Use `new_graph` instead of the constructor to create a new graph instance.

    ```python
    template = GraphSpec().new_template(
        a = (int, lambda x:x),
        b = (str, lambda x:x),
        c = (str, lambda x:x),
    )
    template.a << template.b << template.c
    graph = new_graph(template)
    ```

    In the code above, a graph with three properties (`a`, `b`, `c`) is created, with a structure in which `a` is the parent of `b`, and `b` is the parent of `c`.

    `append` (`replace`) is a method that stores entities in the graph, linking them to each other according to the structure.
    Entities are encapsulated by a `Node`, which can have an edge to a parent node.

    ```python
    graph.append(a=1, b="a", c="x").append(a=2, b="b", c="y")
    ```

    In `append`, entities are first sorted in parent-to-child order, and then:

    - Search the corresponding node container for a node whose entity is *identical* to the first entity.
        - If found, a new node is not created, and the *identical* node is set as the next parent.
        - Otherwise, a new node is appended and set as the next parent.
    - Apply this to the following entities in order. A difference is that the *identical* node is searched for among the sequence of parents accumulated in this operation.

    In this example, the identification is done by the entity value itself (`lambda x:x`). The next example shows where *identical* nodes are found.

    ```python
    graph.append(a=1, b="a", c="z").append(a=1, b="a", c="y")
    ```

    In the second `append`, `a` and `b` each have an *identical* node.
    `c` in the second `append` is not *identical* to any node, because the parent node `b="a"` was already given a new `c` node in the first `append`.

    Due to the identification mechanism, entity relationships in the graph are guaranteed after repeating `append`.
    """
    def __init__(self, template: GraphTemplate):
        #: The graph template.
        self.template: GraphTemplate = template
        #: A `dict` of node containers, keyed by their names.
        self.containers: dict[str, NodeContainer] = {p.name:self._to_container(p) for p in template}
        self._view = None

    def _to_container(self, prop: GraphTemplate.Property) -> 'NodeContainer':
        if isinstance(prop.kind, GraphTemplate):
            return _GraphNodeContainer(prop)
        else:
            return NodeContainer(prop)

    def _container_of(self, prop: GraphTemplate.Property) -> 'NodeContainer | None':
        candidates = [c for c in self.containers.values() if c.prop.is_compatible(prop)]
        if len(candidates) > 1:
            raise ValueError(f"Container can't be determined from property '{prop.name}'.")
        return candidates[0] if candidates else None

    def __add__(self, another: Self | GraphView) -> 'Graph':
        """
        Create new graph by adding this graph and another graph.

        The new graph has the same template as this graph.
        On the other hand, because this method relies on `__iadd__()`, `another` does not need to have the same template.

        Args:
            another: A graph or its view.
        Returns:
            The created graph.
        """
        graph = Graph(self.template)

        graph += self
        graph += another

        return graph

    def __iadd__(self, another: Self | GraphView) -> Self:
        """
        Append nodes from another graph.

        The nodes of another graph are traversed starting from its roots, and appended to the corresponding compatible containers in this graph.

        Args:
            another: A graph or its view.
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
        Returns the root node containers.
        """
        return filter(lambda c: c.prop.parent is None, self.containers.values())

    @property
    def view(self) -> GraphView:
        """
        Returns an unmodifiable view of this graph.

        The view object serves as an accessor for the graph's nodes.

        ```python
        >>> template = GraphSpec().new_template(a=int, b=str, c=str)
        >>> template.a << template.b
        >>> graph = new_graph(template)
        >>> view = graph.view
        >>> assert view() is graph                        # invocation
        >>> assert view.a is graph.containers["a"].view   # attribute
        >>> assert [name for name, _ in view] == ["a", "c"] # iteration
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
        Append entities, keyed by their property names.

        Args:
            entities: The entities, keyed by their property names.
        Returns:
            This graph.
        """
        return self._append(False, entities)

    def replace(self, **entities: Any) -> Self:
        """
        Works similarly to `append`, but the entities of identical nodes are replaced with the given entities.

        Args:
            entities: The entities, keyed by their property names.
        Returns:
            This graph.
        """
        return self._append(True, entities)

    def asdict(self) -> dict[str, list[dict[str, Any]]]:
        """
        Returns a dictionary representation of this graph.

        The keys are the container names, and the values are lists of the node representations for each container.

        Returns:
            A dictionary representation of this graph.
        """
        return {c.name: c.aslist() for c in self.containers.values() if c.prop.parent is None}


def new_graph(template: GraphTemplate, *bases: Graph | GraphView) -> Graph:
    """
    Creates a graph from a template.

    Use this function instead of invoking the constructor directly.

    Args:
        template: The template of the graph.
        bases: Other graphs whose nodes are appended to the created graph.
    Returns:
        The created graph.
    """
    graph = Graph(template)

    for b in bases:
        graph += b

    return graph


class ContainerView[T](Protocol):
    """
    The interface of the view of a node set, i.e., `NodeContainer` and `Node.Children`.
    """
    def __bool__(self) -> bool:
        """Returns whether this container is not empty."""
        ...
    def __call__(self) -> T:
        """Returns the underlying container."""
        ...
    def __len__(self) -> int:
        """Returns the number of nodes."""
        ...
    def __iter__(self) -> Iterator['NodeView']:
        """Iterates the views of the nodes."""
        ...
    @overload
    def __getitem__(self, index: int) -> 'NodeView': ...
    @overload
    def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
    def __getitem__(self, index: int | slice) -> 'NodeView | Iterable[NodeView]':
        """Returns the view of the node at the given index."""
        ...
    def __getattr__(self, key) -> 'ContainerView':
        """Returns the view of the named child container of the first node, or an empty view if this container has no nodes."""
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
        #: The template property.
        self.prop = prop
        self.nodes: list[Node] = []
        self.keys: dict[Any, list[int]] = {}
        self._view = None

    @property
    def name(self) -> str:
        """
        Returns the container name, which is the same as the name of the template property.
        """
        return self.prop.name

    @property
    def view(self) -> ContainerView['NodeContainer']:
        """
        Returns an unmodifiable view of this container.

        The view object serves as an accessor for the container's components.

        ```python
        template = GraphSpec().new_template(a=int, b=str, c=str)
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
                    """Returns the underlying container."""
                    return container
                def __len__(self):
                    """Returns the number of nodes."""
                    return len(container.nodes)
                def __iter__(self):
                    """Iterates the views of the nodes."""
                    return map(lambda n: n.view, container.nodes)
                @overload
                def __getitem__(self, index: int) -> 'NodeView': ...
                @overload
                def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
                def __getitem__(self, index: int | slice) -> 'NodeView | Iterable[NodeView]':
                    """Returns the view of the node at the given index."""
                    if isinstance(index, slice):
                        return [n.view for n in container.nodes[index]]
                    else:
                        return container.nodes[index].view
                def __getattr__(self, key) -> ContainerView:
                    """Returns the view of the named child container of the first node, or an empty view if this container has no nodes."""
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

        An identical node is searched for by checking whether this container already contains a node with an identical entity
        whose parent is found in `ancestors`.

        Args:
            entity: The entity to store in the node.
            ancestors: The parent nodes, mapped by property name.
            to_replace: If `True`, the entity of the identical node is replaced. Otherwise, it is left unchanged.
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

    def aslist(self) -> list[dict[str, Any]]:
        """
        Returns a list containing representations of the nodes in this container.

        Returns:
            A list of representations of the nodes in this container.
        """
        return [n.asdict() for n in self.nodes]


class _GraphNodeContainer(NodeContainer):
    """
    A `NodeContainer` that contains `Graph` instances.
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
        """Returns the entity of this node."""
        ...
    def __getattr__(self, key: str) -> ContainerView:
        """Returns a view of the child nodes for the given property name."""
        ...
    def __iter__(self) -> Iterator[tuple[str, ContainerView]]:
        """Iterates pairs of a child property name and its view."""
        ...


class Node:
    """
    This class represents a node that contains an entity.
    """
    class Children:
        """
        This class represents the child nodes of a node.
        """
        def __init__(self, prop: GraphTemplate.Property):
            #: The template property.
            self.prop = prop
            self.nodes: list[Node] = []
            self.keys = set()
            self._view = None

        @property
        def name(self) -> str:
            """
            Returns the name of the corresponding template property.
            """
            return self.prop.name

        @property
        def view(self) -> ContainerView['Node.Children']:
            """
            Returns an unmodifiable view of the child nodes.
            """
            if self._view is None:
                base = self
                class _ChildrenView:
                    def __bool__(self):
                        """Returns whether this container is not empty."""
                        return len(base.nodes) != 0
                    def __call__(self):
                        """Returns the children container."""
                        return base
                    def __iter__(self):
                        """Iterates the views of the child nodes."""
                        return map(lambda n: n.view, base.nodes)
                    def __len__(self):
                        """Returns the number of child nodes."""
                        return len(base.nodes)
                    @overload
                    def __getitem__(self, index: int) -> 'NodeView': ...
                    @overload
                    def __getitem__(self, index: slice) -> Iterable['NodeView']: ...
                    def __getitem__(self, index):
                        """Returns the view of the child node at the given index."""
                        if isinstance(index, slice):
                            return [n.view for n in base.nodes[index]]
                        else:
                            return base.nodes[index].view
                    def __getattr__(self, key):
                        """Returns the view of the named child container of the first node, or an empty view if this container has no nodes."""
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

    def __init__(self, prop: GraphTemplate.Property, entity: Any, key: Any | None, index: int):
        #: The template property.
        self.prop = prop
        #: The entity held by this node.
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
        Returns the container name, which is the same as the name of the template property.
        """
        return self.prop.name

    @property
    def view(self) -> NodeView:
        """
        Returns an unmodifiable view of this node.

        The view object serves as an accessor for the entity and child nodes.
        """
        if self._view is None:
            node = self
            class _NodeView(NodeView):
                def __call__(self, alt: Any = None) -> Any:
                    """Returns the entity of this node."""
                    # Special key for internal use.
                    if alt is ...:
                        return node
                    return node.entity
                def __getattr__(self, key: str) -> ContainerView:
                    """Returns a view of the child nodes for the given property name."""
                    return node.children[key].view
                def __iter__(self) -> Iterator[tuple[str, ContainerView]]:
                    """Iterates pairs of a child property name and its view."""
                    return map(lambda nc: (nc[0], nc[1].view), node.children.items())
            self._view = _NodeView()
        return self._view

    def add_child(self, child: 'Node') -> Self:
        """
        Adds a child node.

        Args:
            child: The child node to add.
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
        Checks whether this node contains a node identical to the given node.

        Args:
            child: The node to search for.
        Returns:
            `True` if such a node exists.
        """
        if child.prop.template != self.prop.template:
            return False
        elif child.prop.name in self.children:
            return child in self.children[child.prop.name].keys
        else:
            return False

    def asdict(self) -> dict[str, Any]:
        """
        Returns a dictionary representation of this node.

        The entity of this node is stored under the empty string key, and the child nodes are stored under their property names as keys.

        Returns:
            A dictionary representation of this node.
        """
        values: dict[str, Any] = {"": self.entity}
        values.update({c: [n.asdict() for n in ch.nodes] for c, ch in self.children.items()})
        return values


class _GraphNode(Node):
    @property
    def view(self):
        return self.entity.view

    def add_child(self, child):
        raise TypeError(f"GraphNode does not have child.")

    def has_child(self, child):
        return False
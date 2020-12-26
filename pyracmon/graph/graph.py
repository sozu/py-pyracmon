from .identify import neverPolicy
from .template import sort_properties, GraphTemplate


def new_graph(template, *bases):
    """
    Create a graph from a template.

    Use this function instead of invoking constructor directly.

    Parameters
    ----------
    template: GraphTemplate
        A template of a graph.
    bases: [Graph | GraphView]
        Other graphs whose nodes are appended to created graph.

    Returns
    -------
    Graph
        Created graph.
    """
    graph = Graph(template)

    for b in bases:
        graph += b

    return graph


class Graph:
    """
    The instance of this class contains nodes according to the structure defined by the template.

    All entities should be appended via `append()` or `replace()` and these methods not only store nodes but also construct relationships between them.

    Entity set given on an appending session is arranged according to the structure of graph template.
    Relationships between properties are brought into the graph in the form of parent-child relationships between nodes.

    Additionally, each entity value is compared to entities of existing nodes by identification function set on template property,
    and when some entities of nodes are *identical* to given entity, the graph does not create new node
    and will create relationships between those identical nodes and nodes for child entities.

    For example, following template defines that each entity for property `a` are identified by its value and `a` is the parent of `b`.

    >>> template = GraphSpac().new_template(
    >>>     a = (int, lambda x:x),
    >>>     b = str,
    >>> )
    >>> template.a << template.b

    Invocation of `append()` creates nodes and relationships between them.
    Next code shows that each node of `b` becomes a child of node of `a` created in the same invocation.

    >>> graph = new_graph(template)
    >>> graph.append(a=1, b="a").append(a=2, b="b")
    >>>
    >>> assert [(na(), [nb() for nb in na.b]) for na in graph.view.a] == [(1, ["a"]), (2, ["b"])]

    Meanwhile, when an *identical* entity is given to `a`, node of `a` is not created and existing one is used as the parent of node `b`.

    >>> graph.append(a=1, b="c").append(a=1, b="d")
    >>>
    >>> assert [(na(), [nb() for nb in na.b]) for na in graph.view.a] == [(1, ["a", "c", "d"]), (2, ["b"])]
    """
    def __init__(self, template):
        self.template = template
        self.containers = {p.name:self._to_container(p) for p in template._properties}
        self._view = None

    def _to_container(self, prop):
        if isinstance(prop.kind, GraphTemplate):
            return GraphNodeContainer(prop)
        else:
            return NodeContainer(prop)

    def _container_of(self, prop):
        cands = [c for c in self.containers.values() if c.property.is_compatible(prop)]
        if len(cands) > 1:
            raise ValueError(f"Container can't be determined from property '{prop.name}'.")
        return cands[0] if cands else None

    def __add__(self, another):
        """
        Create new graph by adding this graph and another graph.

        New graph has the same template as this graph'S.
        On the other hand, because this method depends on `__iadd__()`, another graph must not have the same template.

        Parameters
        ----------
        another: Graph | GraphView
            Graph or its view.

        Returns
        -------
        Graph
            Created graph.
        """
        graph = Graph(self.template)

        graph += self
        graph += another

        return graph

    def __iadd__(self, another):
        """
        Append nodes from another graph.

        Templates of this graph and another graph must not be the same.
        Nodes of another graph are traversed from its root and appended to compatible containers each other.

        Parameters
        ----------
        another: Graph | GraphView
            Graph or its view.

        Returns
        -------
        Graph
            This graph.
        """
        another = another if isinstance(another, Graph) else another()

        roots_ = filter(lambda c: c.property.parent is None, another.containers.values())

        def add(n, anc):
            c = self._container_of(n.property)
            if c:
                c.append(n.entity, anc)
            for ch_ in n.children.values():
                for m in ch_.nodes:
                    add(m, anc.copy())

        for c_ in roots_:
            for n_ in c_.nodes:
                add(n_, {})

        return self

    @property
    def view(self):
        """
        Returns an unmodifiable view of this graph.

        Returning object provides intuitive ways to access graph components:

        - Attribute access by the property name returns the view of corresponding node container.
        - Iteration access iterates over views of root containers, that is, containers whose properties have no parent.

        See the documentation of `view` property of `NodeContainer`, `Node`, `Node.Children` for further information.

        Returns
        -------
        GraphView
            The view of this graph.
        """
        if self._view is None:
            graph = self
            class GraphView:
                def __call__(self):
                    """Returns the greph of this view."""
                    return graph
                def __iter__(self):
                    """Iterates views of root containers."""
                    return map(lambda c: (c.name, c.view), filter(lambda c: c.property.parent is None, graph.containers.values()))
                def __getattr__(self, name):
                    """Returns a view of a container of the name."""
                    return graph.containers[name].view
            self._view = GraphView()
        return self._view

    def _append(self, to_replace, entities):
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

    def append(self, **entities):
        """
        Append entity values with associated property names.

        Each key in keyword arguments indicates a property and the paired value is an entity value stored in this graph.

        Parameters
        ----------
        entities: {str: object}
            Dictionary where the key indicates the property name and the value is the entity value.

        Returns
        -------
        Graph
            This graph.
        """
        return self._append(False, entities)

    def replace(self, **entities):
        """
        Behaves like `append()` but replace the entities of identiccal nodes.

        Parameters
        ----------
        entities: {str: object}
            Dictionary where the key indicates the property name and the value is the entity value.

        Returns
        -------
        Graph
            This graph.
        """
        return self._append(True, entities)


class _EmptyNodeView:
    def __init__(self, prop, result):
        self.property = prop
        self.result = result

    def __call__(self, alt=None):
        return self.result

    def __iter__(self):
        return iter([])

    def __getattr__(self, key):
        child = next(filter(lambda c: c.name == key, self.property.children), None)
        if child:
            return _EmptyContainerView(child)
        else:
            raise KeyError(f"Graph property '{base.property.name}' does not have a child property '{key}'.")


class _EmptyContainerView:
    def __init__(self, prop):
        self.property = prop

    def __bool__(self):
        return False

    def __call__(self):
        return []

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0

    def __getitem__(self, index):
        if isinstance(index, slice):
            return []
        else:
            raise IndexError(f"Index for container '{self.property.name}' is out of range.")

    def __getattr__(self, key):
        child = next(filter(lambda c: c.name == key, self.property.children), None)
        if child:
            return _EmptyContainerView(child)
        else:
            raise KeyError(f"Graph property '{self.property.name}' does not have a child property '{key}'.")


class NodeContainer:
    """
    This class represents a list of nodes for a template property.
    """
    def __init__(self, prop):
        self.nodes = []
        self.keys = {}
        self.property = prop
        self._view = None

    @property
    def name(self):
        """
        Returns a name of the template property.
        """
        return self.property.name

    @property
    def view(self):
        """
        Returns an unmodifiable view of this container.

        Returning object provides intuitive ways to access internal nodes:

        - Iteration access iterates over internal node views.
        - Index access and `len()` works as if it is a list of node views.
        - Attribute access by the property name returns the view of corresponding child node container under the first node.

        See the documentation of `view` property of `Node`, `Node.Children` for further information.

        Returns
        -------
        ContainerView
            The view of this graph.
        """
        if self._view is None:
            container = self
            class ContainerView:
                def __bool__(self):
                    """Returns whether this container is not empty."""
                    return len(container.nodes) != 0
                def __call__(self):
                    """Returns a base container."""
                    return container
                def __len__(self):
                    """Returns the number of nodes."""
                    return len(container.nodes)
                def __iter__(self):
                    """Iterates views of nodes."""
                    return map(lambda n: n.view, container.nodes)
                def __getitem__(self, index):
                    """Returns a view of a node at the index."""
                    if isinstance(index, slice):
                        return [n.view for n in container.nodes[index]]
                    else:
                        return container.nodes[index].view
                def __getattr__(self, key):
                    """Returns a view of the first node or empty container view if it does not exist."""
                    child = next(filter(lambda c: c.name == key, container.property.children), None)
                    if child:
                        return container.nodes[0].children[key].view if len(container.nodes) > 0 else _EmptyContainerView(child)
                    else:
                        raise KeyError(f"Graph property '{container.property.name}' does not have a child property '{key}'.")
            self._view = ContainerView()
        return self._view

    def append(self, entity, ancestors, to_replace=False):
        """
        Add an entity to this container.

        Parameters
        ----------
        entity: object
            An entity to be stored in the node.
        ancestors: {str: [Node]}
            Dictionary which maps property names to nodes appended to ancestor containers.
        to_replace: bool
            If `True`, entities of identical nodes are replaced with given entity.
        """
        def get_nodes(k):
            return [self.nodes[i] for i in self.keys.get(k, [])]

        policy = self.property.policy or neverPolicy()

        key = policy.get_identifier(entity)

        parents, identicals = policy.identify(self.property, [self.nodes[i] for i in self.keys.get(key, [])], ancestors)

        new_nodes = identicals.copy()

        for p in parents:
            index = len(self.nodes)

            node = Node(self.property, entity, key, index)
            self.nodes.append(node)
            if key is not None:
                self.keys.setdefault(key, []).append(index)
            new_nodes.append(node)

            if p is not None:
                p.add_child(node)

        if to_replace:
            for n in identicals:
                n.entity = entity

        ancestors[self.property.name] = new_nodes


class GraphNodeContainer(NodeContainer):
    def append(self, entity, ancestors, to_replace):
        if not isinstance(entity, (dict, Graph)):
            raise ValueError(f"Node of graph only accepts dict or Graph object.")

        policy = self.property.policy or neverPolicy()

        parents, _ = policy.identify(self.property, [], ancestors)

        for p in parents:
            index = len(self.nodes)

            graphs = []

            if p is None or len(p.children[self.name].nodes) == 0:
                g = Graph(self.property.kind)
                node = GraphNode(self.property, g, None, index)
                self.nodes.append(node)

                if p is not None:
                    p.add_child(node)

                graphs.append(g)
            else:
                graphs.extend([n.entity for n in p.children[self.name].nodes])

            for g in graphs:
                if isinstance(entity, dict):
                    g.append(**entity)
                else:
                    g += entity


class Node:
    """
    This class represents a node which contains an entity value.
    """
    class Children:
        def __init__(self, prop):
            self.nodes = []
            self.keys = set()
            self.property = prop
            self._view = None

        @property
        def view(self):
            if self._view is None:
                base = self
                class ChildrenView:
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
                    def __getitem__(self, index):
                        """Returns a view of child node at the index."""
                        if isinstance(index, slice):
                            return [n.view for n in base.nodes[index]]
                        else:
                            return base.nodes[index].view
                    def __getattr__(self, key):
                        """Returns a view of the first node or empty container view if it does not exist."""
                        child = next(filter(lambda c: c.name == key, base.property.children), None)
                        if child:
                            return base.nodes[0].children[key].view if len(base.nodes) > 0 else _EmptyContainerView(child)
                        else:
                            raise KeyError(f"Graph property '{base.property.name}' does not have a child property '{key}'.")
                self._view = ChildrenView()
            return self._view

        def has(self, node):
            return node in self.keys

        def append(self, node):
            if node not in self.keys:
                self.keys.add(node)
                self.nodes.append(node)

    def __init__(self, prop, entity, key, index):
        self.property = prop
        self.entity = entity
        self.key = key
        self.parents = set()
        self.children = {c.name: Node.Children(c) for c in prop.children}
        self._index = index
        self._view = None

    @property
    def name(self):
        return self.property.name

    @property
    def view(self):
        """
        Returns an unmodifiable view of this node.

        Returning object provides intuitive ways to access entity and child nodes:

        - Direct invocation returns the entity value.
        - Iteration access iterates over views of child nodes.
        - Attribute access by the property name returns the view of corresponding child node.

        Returns
        -------
        NodeView
            The view of this node.
        """
        if self._view is None:
            node = self
            class NodeView:
                def __call__(self, alt=None):
                    """Returns an entity of this node."""
                    return node.entity
                def __getattr__(self, name):
                    """Returns a view of child nodes by its name."""
                    return node.children[name].view
                def __iter__(self):
                    """Iterate key-value pairs of child nodes."""
                    return map(lambda nc: (nc[0], nc[1].view), node.children.items())
            self._view = NodeView()
        return self._view

    def add_child(self, child):
        if child.property.template != self.property.template:
            raise ValueError(f"Nodes from difference graph template can't be associated.")
        self.children[child.property.name].append(child)
        child.parents.add(self)
        return self

    def has_child(self, child):
        if child.property.template != self.property.template:
            return False
        elif child.property.name in self.children:
            return child in self.children[child.property.name].keys
        else:
            return False


class GraphNode(Node):
    @property
    def view(self):
        return self.entity.view

    def add_child(self, child):
        raise TypeError(f"GraphNode does not have child.")

    def has_child(self, child):
        return False
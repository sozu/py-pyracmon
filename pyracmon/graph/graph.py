from collections import OrderedDict
from itertools import chain
from functools import reduce


def new_graph(template):
    """
    Create a graph from a template.

    Use this function instead of invoking constructor directly.

    Parameters
    ----------
    template: GraphTemplate
        A template of a graph.

    Returns
    -------
    Graph
        Created graph.
    """
    return Graph(template)


class Graph:
    """
    The instance of this class contains nodes according to the structure defined by the template.

    All entities are appended via `append()` and this method does many things to construct relationships between nodes.

    - Applies entity filters if any and discards ones which does not fulfill the condition.
    - Searches the identical entity from existing nodes, then, if exists, drops new entity and takes the found one for edge creation.
    - Identification is not only by the entity value but the identicalness of the parent entity in accordance `IdentifyPolicy`.
    - Creates edges between selected nodes according to the relationships of template properties.

    By default, the identification policy is simple, which consider entities are identical only when:

    - Identifier for the template property is found.
    - Values returned by the identitier are equal.
    - They have the same parent nodes.
    """
    def __init__(self, template):
        self.containers = OrderedDict([(p.name, NodeContainer(p)) for p in template._properties])
        self._view = None

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
        if not self._view:
            graph = self
            class GraphView:
                def __iter__(self):
                    """Iterates views of root containers."""
                    return map(lambda c: c.view, filter(lambda c: c.property.parent is None, graph.containers.values()))
                def __getattr__(self, name):
                    """Returns a view of a container of the name."""
                    return graph.containers[name].view
            self._view = GraphView()
        return self._view

    def append(self, **entities):
        """
        Append entity values with associated property names.

        Parameters
        ----------
        entities: {str: object}
            Dictionary where the key indicates the property name and the value is the entity value.
        """
        props = sorted([self.containers[k].property for k in entities.keys()], reverse=True)

        filtered = set()
        for p in props:
            if p.parent is None or (p.parent.name not in entities or p.parent.name in filtered):
                if p.entity_filter is None or p.entity_filter(entities[p.name]):
                    filtered.add(p.name)

        keys = [p.name for p in props if p.name in filtered]

        nodes = {}
        for k in keys:
            ns, edges = self.containers[k].append(entities[k], nodes)
            for c, p in edges:
                p.add_child(c)
            nodes[k] = ns


class IdentifyPolicy:
    def __init__(self, identifier, policy):
        self.identifier = identifier or (lambda x: None)
        self.policy = policy

    def identify(self, prop, entity, candidates, new_nodes):
        """
        Find identical nodes for the new entity according to the policy.

        Parameters
        ----------
        prop: GraphTemplate.Property
            Template property for the container.
        entity: object
            A value to append into the container.
        candidates: object -> [Node]
            Function returning nodes from a identifying key.
        new_nodes: {str: Node}
            Ancestor nodes in this appending session.

        Returns
        -------
        object
            Key of the entity.
        [Node]
            Identical nodes.
        [Node]
            Parent nodes in session where new node should be appended.
        """
        key = self.identifier(entity)
        if key is not None:
            # acc: ([Node], set(Node), [(Node, Node)])
            def merge(acc, c):
                is_identical, parents, edges = self.policy(c, new_nodes)
                if is_identical:
                    acc[0].append(c)
                    for p in parents:
                        acc[1].add(p)
                    for e in edges:
                        acc[2].append(e)
                return acc
            identicals, parents, new_edges = reduce(merge, candidates(key), ([], set(), []))
            return key, identicals, parents, new_edges
        else:
            return None, [], [], []

    @classmethod
    def never(cls):
        return IdentifyPolicy(lambda x: None, lambda c, ns: (False, [], []))

    @classmethod
    def always(cls, identifier):
        def policy(candidate, new_nodes):
            session_parents = new_nodes.get(candidate.property.parent.name, [])
            return True, [], [(candidate, sp) for sp in session_parents if not sp.has_child(candidate)] 
        return IdentifyPolicy(identifier, policy)

    @classmethod
    def hierarchical(cls, identifier):
        def policy(candidate, new_nodes):
            """
            Check a node having the same key as appending node.

            Returns
            -------
            bool
                True if this node should be identical as the appending node in this policy.
            [Node]
                Nodes which should be parent of appending node but currently doesn't have.
            [(Node, Node)]
                Edges contains existing nodes.
            """
            p = candidate.property.parent

            if p is None or p.name not in new_nodes:
                # Graph root or session root node is identified just by its key.
                return True, [], []
            else:
                session_parents = new_nodes.get(p.name, [])
                # This node is identical if one of its parents exists in session parents.
                is_identical = any(map(lambda p: p in session_parents, candidate.parents))
                if is_identical:
                    # Select session parents which doesn't contain this candidate.
                    return True, [sp for sp in session_parents if not sp.has_child(candidate)], []
                else:
                    # No existing nodes are identical.
                    return False, [], []
        return IdentifyPolicy(identifier, policy)


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
        return self.property.name

    @property
    def view(self):
        """
        Returns an unmodifiable view of this container.

        Returning object provides intuitive ways to access internal nodes:

        - Direct invocation returns the `NodeContainer` instance.
        - Iteration access iterates over internal node views.
        - Index access and `len()` works as if it is a list of node views.

        See the documentation of `view` property of `Node`, `Node.Children` for further information.

        Returns
        -------
        ContainerView
            The view of this graph.
        """
        if not self._view:
            container = self
            class ContainerView:
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
                    return container.nodes[index].view
            self._view = ContainerView()
        return self._view

    def append(self, entity, new_nodes = {}):
        """
        Add an entity to nodes if the identical node does not exists yet.

        Parameters
        ----------
        entity: object
            An entity to be stored in the node.
        new_nodes: {str: Node}
            Dictionary which maps property names to nodes appended to ancestor containers.

        Returns
        -------
        [Node]
            Appended nodes.
        [(Node, Node)]
            Edges.
        """
        def cand(k):
            return [self.nodes[i] for i in self.keys.get(k, [])]

        key, existings, parents, new_edges = self.property.identifier.identify(
            self.property,
            entity,
            cand,
            new_nodes,
        )

        edges = [(c, p) for c, p in new_edges]

        if len(existings) > 0:
            edges += sum([[(n, p) for n in existings] for p in new_nodes.get(self.property.name, [])], [])
            for p in parents:
                node = Node(self.property, entity, key)
                self.nodes.append(node)
                existings.append(node)
                edges.append((node, p))
            return existings, edges
        else:
            if key is not None:
                self.keys.setdefault(key, []).append(len(self.nodes))
            node = Node(self.property, entity, key)
            self.nodes.append(node)
            edges += [(node, p) for p in new_nodes.get(self.property.parent.name, [])] if self.property.parent else []
            return [node], edges


class Node:
    """
    This class represents a node which contains an entity value.
    """
    class Children:
        def __init__(self):
            self.nodes = []
            self.keys = set()
            self._view = None

        @property
        def view(self):
            if not self._view:
                base = self
                class ChildrenView:
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
                        return base.nodes[index].view
                self._view = ChildrenView()
            return self._view

        def append(self, node):
            if node not in self.keys:
                self.keys.add(node)
                self.nodes.append(node)

    def __init__(self, prop, entity, key):
        self.property = prop
        self.entity = entity
        self.key = key
        self.parents = set()
        self.children = {c.name: Node.Children() for c in prop.children}
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
        if not self._view:
            node = self
            class NodeView:
                def __call__(self):
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
        self.children.setdefault(child.property.name, Node.Children()).append(child)
        child.parents.add(self)

    def has_child(self, child):
        if child.property.name in self.children:
            return child in self.children[child.property.name].keys
        else:
            return False
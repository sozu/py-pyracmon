from collections import OrderedDict
from itertools import chain


def new_graph(template):
    return Graph(template)


class Graph:
    def __init__(self, template):
        """
        Creates a graph by the template.

        Parameters
        ----------
        template: GraphTemplate
            The template of this graph.
        """
        self.containers = OrderedDict([(p.name, NodeContainer(p)) for p in template._properties])
        self._view = None

    @property
    def view(self):
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
        Append entity values with those associated properties.

        Parameters
        ----------
        entities: *{str: object}
            Dictionary where the key indicates the property name and the value is the entity value.
        """
        keys = [p.name for p in sorted([self.containers[k].property for k in entities.keys()], reverse=True)]

        nodes = {}
        for k in keys:
            n, newly = self.containers[k].append(entities[k], nodes)
            nodes[k] = n

        for n in nodes.values():
            parent = n.property.parent
            if parent and parent.name in nodes:
                nodes[parent.name].add_child(n)


class IdentifyPolicy:
    def __init__(self, identifier, policy):
        self.identifier = identifier or (lambda x: None)
        self.policy = policy

    def identify(self, prop, entity, candidates, new_nodes):
        """
        Parameters
        ----------
        prop: Property
            Property for the container.
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
        Node
            Identical node if exists, otherwise None.
        """
        key = self.identifier(entity)
        if key is not None:
            return key, next(filter(lambda c: c.key == key, filter(lambda c: self.policy(c, new_nodes), candidates(key))), None)
        else:
            return None, None

    @classmethod
    def never(cls):
        return IdentifyPolicy(lambda x: None, lambda c, ns: False)

    @classmethod
    def always(cls, identifier):
        return IdentifyPolicy(identifier, lambda c, ns: True)

    @classmethod
    def hierarchical(cls, identifier):
        def policy(candidate, new_nodes):
            if candidate.property.parent:
                parent = new_nodes.get(candidate.property.parent.name, None)
                return any(map(lambda p: p == parent, candidate.parents))
            else:
                return True
        return IdentifyPolicy(identifier, policy)


class NodeContainer:
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
        Add an entity to nodes if the identifying key does not exists yet.

        Parameters
        ----------
        entity: object
            An entity to be stored in the node.
        new_nodes: {str: Node}
            Dictionary which maps property names to nodes appended to ancestor containers.

        Returns
        -------
        Node
            A node having the entity or matched entity added previously.
        bool
            True when the node is newly created. False when the existing node is returned.
        """
        def cand(k):
            return [self.nodes[i] for i in self.keys.get(k, [])]
        key, existing = self.property.identifier.identify(
            self.property,
            entity,
            cand,
            new_nodes,
        )

        if existing:
            return existing, False
        else:
            if key is not None:
                self.keys.setdefault(key, []).append(len(self.nodes))
            node = Node(self.property, entity, key)
            self.nodes.append(node)
            return node, True

        #key = self.property.identifier(entity) if self.property.identifier else None
        #if key is not None:
        #    if key in self.keys:
        #        return self.nodes[self.keys[key]], False
        #    else:
        #        self.keys.setdefault(key, []).append(len(self.nodes))
        #node = Node(self.property, entity, key)
        #self.nodes.append(node)
        #return node, True

class Node:
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
        self.children = {}
        self._view = None

    @property
    def name(self):
        return self.property.name

    @property
    def view(self):
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
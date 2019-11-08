from collections import OrderedDict
from itertools import chain

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
        nodes = dict([(k, self.containers[k].append(v)) for k, v in entities.items()])

        for n in nodes.values():
            parent = n.property.parent
            if parent and parent.name in nodes:
                nodes[parent.name].add_child(n)


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

    def append(self, entity):
        """
        Add an entity to nodes if the identifying key does not exists yet.

        Parameters
        ----------
        entity: object
            An entity to be stored in the node.

        Returns
        -------
        Node
            A node having the entity or matched entity added previously.
        """
        if self.property.identifier:
            key = self.property.identifier(entity)
            if key is not None:
                if key in self.keys:
                    return self.nodes[self.keys[key]]
                else:
                    self.keys[key] = len(self.nodes)
        node = Node(self.property, entity)
        self.nodes.append(node)
        return node

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

    def __init__(self, prop, entity):
        self.property = prop
        self.entity = entity
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
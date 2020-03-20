class GraphTemplate:
    """
    This class specifies the structure of a graph.

    Use `GraphSpec.new_template()` to construct the instance of this class.

    Each instance is composed of template properties each of which corresponds to a node container of a graph.
    Applying shift operator to properties creates the parent-child relationship between them and it corresponds to an edge of a graph.

    For example, next code creates a template composed of 4 properties in which `d` is a child of `c`, and `b` and `c` are children of `a`.

    >>> template = spec.new_template(a = int, b = str, c = int, d = float)
    >>> template.a << [template.b, template.c]
    >>> template.c << template.d
    """
    class Property:
        def __init__(self, template, name, kind, identifier, entity_filter, origin=None):
            self.template = template
            self.name = name
            self.kind = kind
            self.identifier = identifier
            self.entity_filter = entity_filter
            self._origin = origin

        def _assert_canbe_parent(self, another):
            if another.parent is not None:
                raise ValueError(f"Graph template property can not have multiple parents.")
            if self.template != another.template:
                raise ValueError(f"Properties can make parent-child relationship only when they are declared in the same template.")
            if self == another:
                raise ValueError(f"Recursive relationship is not allowed.")
            p = self
            while p.parent is not None:
                if p.parent == another:
                    raise ValueError(f"Recursive relationship is not allowed.")
                p = p.parent

        @property
        def parent(self):
            return next(filter(lambda r: r[0] == self, self.template._relations), (None, None))[1]

        @property
        def children(self):
            return [r[0] for r in self.template._relations if r[1] == self]

        @property
        def origin(self):
            p = self
            while p._origin:
                p = p._origin
            return p

        def is_compatible(self, other):
            return self.origin is other.origin

        def __lt__(self, other):
            p = self.parent
            while p is not None:
                if p == other:
                    return True
                p = p.parent
            return False
            
        def __lshift__(self, children):
            """
            Makes this property as a parent of child properties.

            Parameters
            ----------
            children: [Graph.Property] | Graph.Property
                Properties to be children of this property.

            Returns
            -------
            [Graph.Property] | Graph.Property
                Child properties given in the argument.
            """
            children = [children] if isinstance(children, GraphTemplate.Property) else children
            for c in children:
                self._assert_canbe_parent(c)
            self.template._relations += [(c, self) for c in children]
            return children[0] if len(children) == 1 else children

        def __rshift__(self, parent):
            """
            Makes this property as a child of another property.

            Parameters
            ----------
            parent: Graph.Property
                A Property to be a parent of this property.

            Returns
            -------
            Graph.Property
                A parent property given in `parent` argument.
            """
            parent._assert_canbe_parent(self)
            self.template._relations += [(self, parent)]
            return parent

    def __init__(self, definitions):
        """
        Construct template with its properties.

        Parameters
        ----------
        definitions: [(str, type, T -> ID, T -> bool)]
            Definitions of template properties.
        """
        self._properties = [GraphTemplate.Property(self, n, kind, ident, ef) for n, kind, ident, ef in definitions]
        self._relations = []
        for p in self._properties:
            setattr(self, p.name, p)


class P:
    @classmethod
    def of(cls, kind=None, identifier=None, entity_filter=None):
        return P(kind, identifier, entity_filter)

    def __init__(self, kind, identifier, entity_filter):
        self.kind = kind
        self.identifier = identifier
        self.entity_filter = entity_filter

    def build(self, name, template):
        return GraphTemplate.Property(template, name, self.kind, self.identifier, self.entity_filter)

    def identify(self, identifier):
        self.identifier = identifier
        return self

    def accept(self, entity_filter):
        self.entity_filter = entity_filter
        return self
class GraphTemplate:
    class Property:
        def __init__(self, template, name, kind, identifier):
            self.template = template
            self.name = name
            self.kind = kind
            self.identifier = identifier

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
            [Graph.Property] / Graph.Property
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
        definitions: [(str, type, (T) -> object)]
            Property definitions.
        """
        self._properties = [GraphTemplate.Property(self, n, kind, ident) for n, kind, ident in definitions]
        self._relations = []
        for p in self._properties:
            setattr(self, p.name, p)
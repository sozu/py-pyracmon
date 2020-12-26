class GraphTemplate:
    """
    This class specifies the structure of a graph.

    Each instance is composed of template properties each of which corresponds to a node container of a graph.
    Applying shift operator to properties creates the parent-child relationship between them and it corresponds to an edge of a graph.

    For example, next code creates a template composed of 4 properties in which `d` is a child of `c`, and `b` and `c` are children of `a`.

    >>> template = GraphSpec().new_template(a=int, b=str, c=int, d=float)
    >>> template.a << [template.b, template.c]
    >>> template.c << template.d
    """
    class Property:
        def __init__(self, template, name, kind, policy, entity_filter, origin=None):
            self.template = template
            self.name = name
            self.kind = kind
            self.policy = policy
            self.entity_filter = entity_filter
            self._origin = origin

        def _assert_canbe_parent(self, another):
            if another.parent is not None:
                raise ValueError(f"Graph template property can not have multiple parents.")
            if self.template != another.template:
                raise ValueError(f"Properties can make parent-child relationship only when they are declared in the same template.")
            if self == another:
                raise ValueError(f"Recursive relationship is not allowed.")
            if isinstance(self.kind, GraphTemplate):
                raise ValueError(f"Property for graph template can't have child.")
            p = self
            while p.parent is not None:
                if p.parent == another:
                    raise ValueError(f"Recursive relationship is not allowed.")
                p = p.parent

        @property
        def parent(self):
            """
            Returns parent property.

            Returns
            -------
            GraphTemplate.Property
                Parent property if exists, otherwise `None`.
            """
            return next(filter(lambda r: r[0] == self, self.template._relations), (None, None))[1]

        @property
        def children(self):
            """
            Returns child properties.

            Returns
            -------
            [GraphTemplate.Property]
                Child properties.
            """
            return [r[0] for r in self.template._relations if r[1] == self]

        @property
        def origin(self):
            p = self
            while p._origin:
                p = p._origin
            return p

        def is_compatible(self, other):
            return self.origin is other.origin

        def move_template(self, dest, new_name=None):
            new_name = new_name or self.name
            prop = GraphTemplate.Property(dest, new_name, self.kind, self.policy, self.entity_filter, origin=self)
            _set_template_property(dest, prop)
            for c in self.children:
                cc = c.move_template(dest)
                prop << cc
            return prop

        def __lshift__(self, children):
            """
            Makes this property as a parent of child properties.

            Parameters
            ----------
            children: [GraphTemplate.Property] | GraphTemplate.Property
                Properties to be children of this property.

            Returns
            -------
            [GraphTemplate.Property] | GraphTemplate.Property
                Child properties given in the argument.
            """
            targets = [children] if isinstance(children, GraphTemplate.Property) else children
            for c in targets:
                self._assert_canbe_parent(c)
            self.template._relations += [(c, self) for c in targets]
            return children

        def __rshift__(self, parent):
            """
            Makes this property as a child of another property.

            Parameters
            ----------
            parent: GraphTemplate.Property
                A Property to be a parent of this property.

            Returns
            -------
            GraphTemplate.Property
                A parent property given in `parent` argument.
            """
            parent._assert_canbe_parent(self)
            self.template._relations += [(self, parent)]
            return parent

        def __rrshift__(self, children):
            """
            Reversed version of `__lshift__()` prepared to locate a list of properties on the left side.

            Parameters
            ----------
            children: [GraphTemplate.Property] | GraphTemplate.Property
                Properties to be children of this property.

            Returns
            -------
            GraphTemplate.Property
                This property.
            """
            self.__lshift__(children)
            return self

    def __init__(self, definitions):
        """
        Construct template with its properties.

        Parameters
        ----------
        definitions: [(str, type, T -> ID, T -> bool)]
            Definitions of template properties.
        """
        self._properties = []
        self._relations = []

        for d in definitions:
            it = iter(d)
            name, kind, ident, ef = (next(it), next(it), next(it, None), next(it, None))

            if isinstance(kind, GraphTemplate):
                prop = GraphTemplate.Property(self, name, kind, None, None)
                _set_template_property(self, prop)
            elif isinstance(kind, GraphTemplate.Property):
                kind.move_template(self, name)
            else:
                _set_template_property(self, GraphTemplate.Property(self, name, kind, ident, ef))

    def __iter__(self):
        """
        Iterate properties in parent-to-child order.

        Returns
        -------
        Iterator[GraphTemplate.Property]
            Property iterator.
        """
        return sort_properties(self._properties)

    def __iadd__(self, another):
        """
        Add another template to this template.

        Parameters
        ----------
        another: GraphTemplate
            Another template.

        Returns
        -------
        GraphTemplate
            This instance.
        """
        for p in another._properties:
            prop = GraphTemplate.Property(self, p.name, p.kind, p.policy, p.entity_filter, origin=p)
            _set_template_property(self, prop)

        for n, p in another._relations:
            getattr(self, n.name) >> getattr(self, p.name)

        return self

    def __add__(self, another):
        """
        Create new template by adding this template and another one.

        Parameters
        ----------
        another: GraphTemplate
            Another template.

        Returns
        -------
        GraphTemplate
            New template.
        """
        template = GraphTemplate([])
        template += self
        template += another

        return template


def _set_template_property(template, prop):
    if hasattr(template, prop.name):
        raise ValueError(f"Property name '{prop.name}' conflicts.'")
    template._properties.append(prop)
    setattr(template, prop.name, prop)


def sort_properties(properties, parent=None):
    names = {p.name for p in properties}

    def walk(p):
        yield p
        for q in p.children:
            for r in walk(q):
                yield r

    for p in filter(lambda p: p.parent is parent, properties):
        for q in walk(p):
            yield q
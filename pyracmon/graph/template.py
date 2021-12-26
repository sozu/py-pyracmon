from typing import *

from pyracmon.graph.identify import IdentifyPolicy


class GraphTemplate:
    """
    This class specifies the structure of a graph.

    The template is composed of template properties each of which corresponds to a node container of a graph.
    Each template property can be obtained via an attribute of its name from the template.

    Applying shift operator between properties creates the parent-child relationship between them.
    In next code, the template is composed of 4 properties where `d` is a child of `c`, and `b` and `c` are children of `a`.

    >>> template = GraphSpec().new_template(a=int, b=str, c=int, d=float)
    >>> template.a << [template.b, template.c]
    >>> template.c << template.d

    Templates are merged when `+` is applied to them. The result has properties defined in both templates with keeping their relationships.
    Merging of templates having properties of the same name fails by raising `ValueError`.

    Use `GraphSpec.new_template` or other factory functions to create a template instead of using constructor directly.
    """
    class Property:
        """
        Template property which determines various behaviors of graph nodes.
        """
        def __init__(
            self,
            template: 'GraphTemplate',
            name: str,
            kind: type,
            policy: Optional[IdentifyPolicy],
            entity_filter: Optional[Callable[[Any], bool]],
            origin = None,
        ):
            #: Graph template this property belongs to.
            self.template = template
            #: Property name.
            self.name = name
            #: Graph node bound to this property should have entity of this type.
            self.kind = kind
            #: Policy of entity identification.
            self.policy = policy
            #: Entity filter function.
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
        def parent(self) -> Optional['GraphTemplate.Property']:
            """
            Returns parent property if exists.
            """
            return next(filter(lambda r: r[0] == self, self.template._relations), (None, None))[1]

        @property
        def children(self) -> List['GraphTemplate.Property']:
            """
            Returns child properties.
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

        def _move_template(self, dest, new_name=None):
            new_name = new_name or self.name
            prop = GraphTemplate.Property(dest, new_name, self.kind, self.policy, self.entity_filter, origin=self)
            _set_template_property(dest, prop)
            for c in self.children:
                cc = c._move_template(dest)
                prop << cc
            return prop

        def __lshift__(
            self,
            children: Union['GraphTemplate.Property', List['GraphTemplate.Property']],
        ) -> Union['GraphTemplate.Property', List['GraphTemplate.Property']]:
            """
            Makes this property as a parent of child properties.

            Args:
                children: Property or properties to be children of this property.
            Returns:
                The same object as the argument.
            """
            targets = [children] if isinstance(children, GraphTemplate.Property) else children
            for c in targets:
                self._assert_canbe_parent(c)
            self.template._relations += [(c, self) for c in targets]
            return children

        def __rshift__(self, parent: 'GraphTemplate.Property') -> 'GraphTemplate.Property':
            """
            Makes this property as a child of another property.

            Args:
                parent: A Property to be a parent of this property.
            Returns:
                The same object as the argument.
            """
            parent._assert_canbe_parent(self)
            self.template._relations += [(self, parent)]
            return parent

        def __rrshift__(
            self,
            children: Union['GraphTemplate.Property', List['GraphTemplate.Property']],
        ) -> Union['GraphTemplate.Property', List['GraphTemplate.Property']]:
            """
            Reversed version of `__lshift__()` prepared to locate a list of properties on the left side.

            Args:
                children: Property or properties to be children of this property.
            Returns:
                The same object as the argument.
            """
            self.__lshift__(children)
            return self

    def __init__(self, definitions: List[Tuple[str, type, Callable[[Any], Any], Callable[[Any], bool]]]):
        """
        Construct template with its properties.  Don't use this constructor directly.

        Args:
            definitions: Definitions of template properties.
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
                kind._move_template(self, name)
            else:
                _set_template_property(self, GraphTemplate.Property(self, name, kind, ident, ef))

    def __iter__(self) -> Iterator['GraphTemplate.Property']:
        """
        Iterates properties in parent-to-child order.

        Returns:
            Property iterator.
        """
        return _sort_properties(self._properties)

    def __iadd__(self, another: 'GraphTemplate') -> 'GraphTemplate':
        """
        Add another template to this template.

        Args:
            another: Another template.
        Returns:
            This instance.
        """
        for p in another._properties:
            prop = GraphTemplate.Property(self, p.name, p.kind, p.policy, p.entity_filter, origin=p)
            _set_template_property(self, prop)

        for n, p in another._relations:
            getattr(self, n.name) >> getattr(self, p.name)

        return self

    def __add__(self, another: 'GraphTemplate') -> 'GraphTemplate':
        """
        Create new template by merging this template and another one.

        Args:
            another: Another template.
        Returns:
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


def _sort_properties(properties, parent=None):
    names = {p.name for p in properties}

    def walk(p):
        yield p
        for q in p.children:
            for r in walk(q):
                yield r

    for p in filter(lambda p: p.parent is parent, properties):
        for q in walk(p):
            yield q
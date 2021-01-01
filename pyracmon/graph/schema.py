import sys
from typing import get_type_hints, Generic, TypeVar, List
from inspect import signature, Signature


T = TypeVar('T')


if sys.version_info[0:2] >= (3, 8):
    from typing import TypedDict, get_args, get_origin
else:
    class TypedDict(dict):
        pass
    def get_args(tp):
        return tp.__args__
    def get_origin(tp):
        return tp.__origin__


def issubgeneric(t, p):
    """
    Check whether a type is subclass of a generic type.

    Parameters
    ----------
    t: type
        A type to check.
    p: type
        A generic type.

    Returns
    -------
    bool
        Whether the type in subclass of the generic type.
    """
    if type(t) is type:
        return issubclass(t, p)
    else:
        return hasattr(t, '__origin__') and issubclass(get_origin(t), p)


class DocumentedType(Generic[T]):
    """
    Represents a type having documentation as doc-string.
    """
    @staticmethod
    def unpack(dt):
        """
        Unpack a `DocumentedType` into type and documentation.

        Parameters
        ----------
        dt: type(DocumentedType)
            A type of `DocumentedType`.

        Returns
        -------
        type
            Type of the `DocumentedType`.
        str
            Documentation of the `DocumentedType`.
        """
        t = get_args(dt)[0]
        return t, dt.__doc__


def document_type(t, doc):
    """
    Creates a `DocumentedType` for given type and documentation.

    Parameters
    ----------
    t: type
        The type of `DocumentedType`
    doc: string
        The documentation of `DocumentedType`

    Returns
    -------
    Type[DocumentedType]
        Created `DocumentedType`.
    """
    if issubgeneric(t, DocumentedType):
        t = get_args(t)[0]
    class Documented(DocumentedType[T]):
        pass
    Documented[t].__doc__ = doc or ""
    return Documented[t]


class Typeable(Generic[T]):
    """
    An interface for generic type which is resolved into a concrete type by a type parameter.

    Inherit this class and declare static method whose signature is `resolve(me, bound, arg)`.

    >>> class A(Typeable[T]):
    >>>     @staticmethod
    >>>     def resolve(me, bound, arg):
    >>>         ...
    >>>         return some_type
    >>>
    >>> Typeable.resolve(A[T], int)

    Type resolution should start from the invocation of `Typeable.resolve()`.
    It subsequently invokes the static method with arguments below.

    - Typeable type to resolve whose type parameter is replaced with concrete type or another resolved Typeable type.
    - Resolved type for the type parameter.
    - A type passed by the invocation of `Typeable.resolve()`.
    """
    @staticmethod
    def resolve(typeable, arg):
        """
        Resolve a `Typeable` type by a type.

        Parameters
        ----------
        typeable: type[Typeable[T]]
            `Typeable` type having a generic type parameter.
        arg: type
            Type to replace a type variable.

        Returns
        -------
        type
            Resolved type.
        """
        if get_origin(typeable) is Typeable:
            raise ValueError(f"Typeable should not be used directly. Use inheriting class instead.")

        bound = get_args(typeable)[0]

        if isinstance(bound, TypeVar):
            return Typeable.resolve(typeable[arg], arg)
        elif issubgeneric(bound, Typeable):
            bound = Typeable.resolve(bound, arg)
            return typeable.resolve(typeable, bound, arg)
        else:
            return typeable.resolve(typeable, bound, arg)

    @staticmethod
    def is_resolved(typeable):
        """
        Checks a `Typeable` type is already resolved.

        Parameters
        ----------
        typeable: type[Typeable[T]]
            `Typeable` type having a generic type parameter.

        Returns
        -------
        bool
            Whether the type is already resolved or not.
        """
        bound = get_args(typeable)[0]
        if isinstance(bound, TypeVar):
            return False
        elif issubgeneric(bound, Typeable):
            return Typeable.is_resolved(bound)
        else:
            return True


class DynamicType(Typeable[T]):
    @staticmethod
    def resolve(dynamic, bound, arg):
        return dynamic.fix(bound, arg)

    @classmethod
    def fix(cls, bound, arg):
        return bound


class Shrink(Typeable[T]):
    @staticmethod
    def resolve(shrink, bound, arg):
        if bound == Signature.empty:
            return TypedDict
        if not issubclass(bound, TypedDict):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {base}.")

        class Schema(TypedDict):
            pass
        exc, inc = shrink.select(bound, arg)
        setattr(Schema, '__annotations__', {n:t for n, t in get_type_hints(bound).items() if (not inc or n in inc) and (n not in exc)})

        return Schema

    @classmethod
    def select(cls, bound, arg):
        raise NotImplementedError()


class Extend(Typeable[T]):
    @staticmethod
    def resolve(extend, bound, arg):
        if bound == Signature.empty:
            return TypedDict
        if not issubclass(bound, TypedDict):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {base}.")

        class Schema(extend.schema(bound, arg), bound):
            pass

        return Schema

    @classmethod
    def schema(cls, bound, arg):
        raise NotImplementedError()


def walk_schema(td, with_doc=False):
    """
    Returns a dictionary as a result of walking a schema object from its root.

    Parameters
    ----------
    td: TypedDict
        A schema represented by `TypedDict`.
    with_doc: bool
        Flag to include documentations into result.

    Returns
    -------
    dict
        Dictionary representing the schema.
    """
    if '__annotations__' not in td.__dict__:
        return {}

    result = {}

    def put(k, t, doc):
        if with_doc:
            result[k] = (t, doc)
        else:
            result[k] = t

    def expand(t):
        return (get_args(t)[0], lambda x:[x]) if issubgeneric(t, List) else (t, lambda x:x)

    for k, t in get_type_hints(td).items():
        t, doc = DocumentedType.unpack(t) if issubgeneric(t, DocumentedType) else (t, "")

        t, conv = expand(t)

        if issubclass(t, TypedDict):
            put(k, conv(walk_schema(t, with_doc)), doc)
        else:
            put(k, conv(t), doc)
    
    return result


class GraphSchema:
    """
    This class provides property to get the schema of serialization result of a graph as well as serialization method.
    """
    def __init__(self, spec, template, **serializers):
        self.spec = spec
        self.template = template
        self.serializers = serializers

    def _return_from(self, prop):
        ns = self.serializers[prop.name]

        # Return type of serializers set to NodeSerializer.
        rt = signature(ns.serializer).return_annotation

        # Return type of base serializer obtained from GraphSpec.
        base = self.spec.get_serializer(prop.kind)

        bt = signature(base).return_annotation if base else Signature.empty
        bt = prop.kind if bt == Signature.empty else bt

        if rt == Signature.empty:
            rt = bt
            bt = prop.kind

        if issubgeneric(bt, Typeable):
            bt = bt if Typeable.is_resolved(bt) else bt[prop.kind]

        if issubgeneric(rt, Typeable):
            if not Typeable.is_resolved(rt):
                rt = rt[bt]
            rt = Typeable.resolve(rt, prop.kind)

        return rt

    def schema_of(self, prop):
        """
        Generates structured and documented schema for a template property.

        Parameters
        ----------
        prop: GraphTemplate.Property
            A template property.

        Returns
        -------
        Type[DocumentedType]
            Schema and its documentation.
        """
        rt = self._return_from(prop)

        doc = self.serializers[prop.name]._doc or ""

        if issubclass(rt, dict):
            class Schema(rt):
                pass

            anns = {}

            for c in filter(lambda c: c.name in self.serializers, prop.children):
                ns = self.serializers[c.name]
                cs = self.schema_of(c)

                t, d = DocumentedType.unpack(cs)

                if ns.be_merged:
                    if not issubclass(t, dict):
                        raise ValueError(f"Property '{c.name}' is not configured to be serialized into dict.")
                    anns.update(**{ns.namer(k):t for k, t in get_type_hints(t).items()})
                else:
                    anns[ns.namer(c.name)] = cs if ns.be_singular else document_type(List[t], d)

            setattr(Schema, '__annotations__', anns)

            return document_type(Schema, doc)
        else:
            return document_type(rt, doc)

    @property
    def schema(self):
        """
        Generates `TypedDict` which represents the schema of serialized graph.

        Returns
        -------
        TypedDict
            Representation of the schema of serialized graph.
        """
        class Schema(TypedDict):
            pass

        def root_schema_of(p):
            dt = self.schema_of(p)
            if self.serializers[p.name].be_singular:
                return dt
            else:
                t, d = DocumentedType.unpack(dt)
                return document_type(List[t], d)

        roots = filter(lambda p: p.parent is None and p.name in self.serializers, self.template._properties)

        setattr(Schema, '__annotations__', {self.serializers[p.name].namer(p.name):root_schema_of(p) for p in roots})

        return Schema

    def serialize(self, graph):
        """
        Serialize graph into a dictionary.

        Parameters
        ----------
        graph: GraphView
            A graph.

        Returns
        -------
        dict
            Serialized representation of the graph.
        """
        return self.spec.to_dict(graph, **self.serializers)
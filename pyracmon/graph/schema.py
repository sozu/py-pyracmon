import sys
from typing import Type, get_type_hints, Generic, TypeVar, Any, List, Dict, Tuple, Union
from inspect import signature, Signature
from .template import GraphTemplate
from .util import chain_serializers, T


if sys.version_info[0:2] >= (3, 8):
    from typing import get_args, get_origin
elif sys.version_info[0:2] <= (3, 6):
    def get_args(tp):
        def bind(base, args):
            if isinstance(args, tuple):
                t, subs = args
                return base[bind(t, subs)]
            else:
                return base[args]
        
        _, args = tp._subs_tree()
        if isinstance(args, tuple):
            b, a = args
            return [bind(b, a)]
        else:
            return [args]
    def get_origin(tp):
        return tp.__origin__
else:
    def get_args(tp):
        return tp.__args__
    def get_origin(tp):
        return tp.__origin__


# TODO Should support TypedDict exported from typing package.
# In cpython <= 3.9, TypedDict has not yet supported subclass check by issubclass().
class TypedDict(dict):
    pass


def issubgeneric(t: type, p: type) -> bool:
    """
    Check whether a type is subclass of a generic type.

    :param t: A type to check.
    :param p: A generic type.
    :returns: Whether the type in subclass of the generic type.
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
    def unpack(dt: Type['DocumentedType']) -> Tuple[type, str]:
        """
        Unpack a `DocumentedType` into type and documentation.

        :param dt: A type of `DocumentedType`.
        :returns: Unpacked type and documentation of given type.
        """
        t = get_args(dt)[0]
        return t, dt.__doc__


def document_type(t: type, doc: str) -> Type[DocumentedType]:
    """
    Creates a `DocumentedType` for given type and documentation.

    :param t: A type to give the documentaion.
    :param doc: The documentation string.
    :returns: Created `DocumentedType`.
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

    Inherit this class and declare static method whose signature is ``resolve(me, bound, arg, spec) -> type`` .

    >>> class A(Typeable[T]):
    >>>     @staticmethod
    >>>     def resolve(me, bound, arg, spec):
    >>>         ...
    >>>         return some_type
    >>>
    >>> Typeable.resolve(A[T], int, spec)

    Type resolution starts from `Typeable.resolve` which invokes the static method with following arguments.

    - Type to resolve itself, in this case, ``A[T]`` .
    - A resolved type which replace ``T`` .
        - ``arg`` is the first candidate.
        - When ``arg`` is also `Typeable` , this resolution flow is applied to it recursively until concrete type if determined.
    - ``arg`` is passed through as it is.
    - ``spec`` is passed through as it is.
    """
    @staticmethod
    def resolve(typeable: Type['Typeable'], arg: type, spec: 'GraphSpec') -> type:
        """
        Resolve a `Typeable` type into a concrete type by a type for its type parameter.

        :param typeable: `Typeable` type having a generic type parameter.
        :param arg: Type to replace a type parameter.
        :param spec: `GraphSpec` used for schema generation.
        :returns: Resolved type.
        """
        if get_origin(typeable) is Typeable:
            raise ValueError(f"Typeable should not be used directly. Use inheriting class instead.")

        bound = get_args(typeable)[0]

        if isinstance(bound, TypeVar):
            return Typeable.resolve(typeable[arg], arg, spec)
        elif issubgeneric(bound, Typeable):
            bound = Typeable.resolve(bound, arg, spec)
            return typeable.resolve(typeable, bound, arg, spec)
        else:
            return typeable.resolve(typeable, bound, arg, spec)

    @staticmethod
    def is_resolved(typeable: Type['Typeable']) -> bool:
        """
        Checks a type parameter of given `Typeable` is alredy resolved.

        :param typeable: `Typeable` type having a generic type parameter.
        :returns: Whether the type parameter is already resolved or not.
        """
        bound = get_args(typeable)[0]
        if isinstance(bound, TypeVar):
            return False
        elif issubgeneric(bound, Typeable):
            return Typeable.is_resolved(bound)
        else:
            return True


class DynamicType(Typeable[T]):
    """
    A `Typeable` type which can be resolved dynamically with resolved type parameter.
    """
    @staticmethod
    def resolve(dynamic: Type['DynamicType'], bound: type, arg: type, spec: 'GraphSpec') -> type:
        return dynamic.fix(bound, arg)

    @classmethod
    def fix(cls, bound: type, arg: type) -> type:
        """
        Resolve a resolved type into another type.

        Override this method to apply specific logic of the inheriting type to resolved type.
        ex) Convert resolved model type into `TypedDict` for serialization.

        :param bound: Resolved type of `T`.
        :param arg: A type used for the resolution of `bound`.
        :returns: Another type.
        """
        return bound


class Shrink(Typeable[T]):
    """
    A type to remove keys from `TypedDict` bound to the type parameter `T`.

    This class only works when `TypedDict` parameter is set, otherwise `TypeError` is raised.
    """
    @staticmethod
    def resolve(shrink: Type['Shrink'], bound: type, arg: type, spec: 'GraphSpec') -> type:
        """
        Resolve a `TypedDict` into another `TypedDict` by removing some keys defined by `select` .
        """
        if bound == Signature.empty:
            return TypedDict
        if not issubclass(bound, TypedDict):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")

        class Schema(TypedDict):
            pass
        exc, inc = shrink.select(bound, arg)
        setattr(Schema, '__annotations__', {n:t for n, t in get_type_hints(bound).items() if (not inc or n in inc) and (n not in exc)})

        return Schema

    @classmethod
    def select(cls, bound: Union[TypedDict, Signature], arg: type) -> Tuple[List[str], List[str]]:
        """
        Select excluding and including keys from `TypedDict`.

        Subclass should consider the case when the ``bound`` is `Signautre.empty` .

        This method should return excluding and including keys.
        Excluding key is always excluded if it is contained in including keys.
        Empty including keys specify that all keys are used.

        :param bound: `TypedDict` to be shrinked.
        :param arg: A type used for the resolution of `bound` .
        :returns: Keys to exclude and include.
        """
        raise NotImplementedError()


class Extend(Typeable[T]):
    """
    A type to add keys to `TypedDict` bound to the type parameter `T`.

    This class only works when `TypedDict` parameter is set, otherwise `TypeError` is raised.
    """
    @staticmethod
    def resolve(extend: Type['Extend'], bound: type, arg: type, spec: 'GraphSpec'):
        """
        Resolve a `TypedDict` into another `TypedDict` by adding some keys retrieved by `schema` .
        """
        if bound == Signature.empty:
            return TypedDict
        if not issubclass(bound, TypedDict):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")

        class Schema(extend.schema(bound, arg), bound):
            pass

        return Schema

    @classmethod
    def schema(cls, bound: Union[TypedDict, Signature], arg: type) -> TypedDict:
        """
        Creates a `TypedDict` representing a schema of adding keys and their types.

        Subclass should consider the case when the ``bound`` is `Signautre.empty` .

        Parameters
        ----------
        :param bound: `TypedDict` to be extended.
        :param arg: A type used for the resolution of `bound`.
        :returns: `TypedDict` extending schema of base `TypedDict` .
        """
        raise NotImplementedError()


def walk_schema(td, with_doc=False) -> Dict[str, Union[type, DocumentedType]]:
    """
    Returns a dictionary as a result of walking a schema object from its root.

    :param td: A schema represented by `TypedDict`.
    :param with_doc: Flag to include documentations into result.
    :returns: Key value representation of the schema. If ``with_doc`` is ``True``, each value is `DocumentedType` .
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


def templateType(t):
    class Template:
        template = t
    return Template


class GraphSchema:
    """
    This class exposes a property to get the schema of serialization result of a graph.

    Schema generation also depends on `GraphSpec` , on which `serialize` serializes a graph.

    :param spec: Specification of graph operations.
    :param template: Graph template to serialize.
    :param serializers: `NodeSerializer` s used for the serialization.
    """
    def __init__(self, spec: 'GraphSpec', template: GraphTemplate, **serializers: 'NodeSerializer'):
        self.spec = spec
        self.template = template
        self.serializers = serializers

    def _return_from(self, prop):
        ns = self.serializers[prop.name]

        arg = prop.kind
        arg = templateType(prop.kind) if isinstance(prop.kind, GraphTemplate) else arg

        # Return type of serializers set to NodeSerializer.
        rt = signature(ns.serializer).return_annotation

        # Return type of base serializer obtained from GraphSpec.
        base = chain_serializers(self.spec.find_serializers(arg))

        bt = signature(base).return_annotation if base else Signature.empty
        bt = arg if bt == Signature.empty else bt

        if rt == Signature.empty:
            rt = bt
            bt = arg

        if issubgeneric(bt, Typeable):
            bt = bt if Typeable.is_resolved(bt) else bt[arg]

        if issubgeneric(rt, Typeable):
            if not Typeable.is_resolved(rt):
                rt = rt[bt]
            rt = Typeable.resolve(rt, arg, self.spec)

        return rt

    def schema_of(self, prop: GraphTemplate.Property) -> Type[DocumentedType]:
        """
        Generates structured and documented schema for a template property.

        :param prop: A template property.
        :returns: Schema with documentation.
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
    def schema(self) -> TypedDict:
        """
        Generates `TypedDict` which represents the schema of serialized graph.

        :getter: Representation of the schema of serialized graph.
        """
        class Schema(TypedDict):
            pass

        anns = {}

        def put_root_schema(p):
            nonlocal anns

            ns = self.serializers[p.name]
            dt = self.schema_of(p)

            if ns.be_merged:
                t, d = DocumentedType.unpack(dt)
                anns.update(**{ns.namer(k):t_ for k, t_ in get_type_hints(t).items()})
            elif ns.be_singular:
                anns[ns.namer(p.name)] = dt
            else:
                t, d = DocumentedType.unpack(dt)
                anns[ns.namer(p.name)] = document_type(List[t], d)

        roots = filter(lambda p: p.parent is None and p.name in self.serializers, self.template._properties)

        for p in roots:
            put_root_schema(p)

        setattr(Schema, '__annotations__', anns)

        return Schema

    def serialize(self, graph: 'GraphView', **node_params: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Serialize graph into a dictionary.

        :param graph: A view of a graph.
        :parma node_params: Parameters passed to `SerializationContext` and used by *serializer* s.
        :returns: Serialization result.
        """
        return self.spec.to_dict(graph, node_params, **self.serializers)
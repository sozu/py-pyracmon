import sys
from typing import Type, Generic, TypeVar, Any, List, Dict, Tuple, Union
from inspect import signature, Signature
from .template import GraphTemplate
from .util import chain_serializers, T
from .typing import *


def document_type(t: type, doc: str) -> Type[Annotated]:
    """
    Supplies a document to a type.

    :param t: A type.
    :param doc: A document.
    :returns: Documented type.
    """
    return annotate(t, doc)


def _decompose_document(t: type) -> Tuple[type, str]:
    if get_origin(t) == Annotated:
        t, d = get_args(t)
        return t, d if isinstance(d, str) else d.__doc__
    else:
        return t, ""


class Typeable(Generic[T]):
    """
    An interface for generic type which is resolved into a concrete type by a type parameter.

    Inherit this class and declare static method whose signature is `resolve(me, bound, arg, spec) -> type`.

    >>> class A(Typeable[T]):
    >>>     @staticmethod
    >>>     def resolve(me, bound, arg, spec):
    >>>         ...
    >>>         return some_type
    >>>
    >>> Typeable.resolve(A[T], int, spec)

    Type resolution starts from `Typeable.resolve` which invokes the static method with following arguments.

    - Type to resolve itself, in this case, `A[T]`.
    - A resolved type which replace `T`.
        - `arg` is the first candidate.
        - When `arg` is also `Typeable` , this resolution flow is applied to it recursively until concrete type if determined.
    - `arg` is passed through as it is.
    - `spec` is passed through as it is.
    """
    @staticmethod
    def resolve(typeable, arg: type, spec: 'GraphSpec') -> type:
        """
        Resolve a `Typeable` type into a concrete type by a type for its type parameter.

        Args:
            typeable: `Typeable` type having a generic type parameter.
            arg: Type to replace a type parameter.
            spec: `GraphSpec` used for schema generation.
        Returns:
            Resolved type.
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

        Args:
            typeable: `Typeable` type having a generic type parameter.
        Returns:
            Whether the type parameter is already resolved or not.
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

        Args:
            bound: Resolved type of `T`.
            arg: A type used for the resolution of `bound`.
        Returns:
            Another type.
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
        if not is_typed_dict(bound):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")

        exc, inc = shrink.select(bound, arg)
        annotations = {n:t for n, t in get_annotated_hints(bound).items() if (not inc or n in inc) and (n not in exc)}

        return generate_schema(annotations)

    @classmethod
    def select(cls, bound: Union[TypedDict, Signature], arg: type) -> Tuple[List[str], List[str]]:
        """
        Select excluding and including keys from `TypedDict`.

        Subclass should consider the case when the `bound` is `Signautre.empty`.

        This method should return excluding and including keys.
        Excluding key is always excluded if it is contained in including keys.
        Empty including keys specify that all keys are used.

        Args:
            bound: `TypedDict` to be shrinked.
            arg: A type used for the resolution of `bound`.
        Returns:
            Keys to exclude and include.
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
        if not is_typed_dict(bound):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")

        ext = extend.schema(bound, arg)

        return generate_schema(ext.__annotations__ if is_typed_dict(ext) else {}, bound)

    @classmethod
    def schema(cls, bound: Union[TypedDict, Signature], arg: type) -> Union[TypedDict, Signature.empty]:
        """
        Creates a `TypedDict` representing a schema of adding keys and their types.

        Subclass should consider the case when the `bound` is `Signautre.empty`.

        Args:
            bound: `TypedDict` to be extended.
            arg: A type used for the resolution of `bound`.
        Returns:
            `TypedDict` extending schema of base `TypedDict` .
        """
        raise NotImplementedError()


def walk_schema(td, with_doc=False) -> Dict[str, Union[type, Annotated]]:
    """
    Returns a dictionary as a result of walking a schema object from its root.

    Args:
        td: A schema represented by `TypedDict`.
        with_doc: Flag to include documentations into result.
    Returns:
        Key value representation of the schema. If `with_doc` is `True`, each value is `Annotated`.
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

    for k, t in get_annotated_hints(td).items():
        t, doc = _decompose_document(t)

        t, conv = expand(t)

        if is_typed_dict(t):
            put(k, conv(walk_schema(t, with_doc)), doc)
        else:
            put(k, conv(t), doc)
    
    return result


def _templateType(t):
    class Template:
        template = t
    return Template


class GraphSchema:
    """
    This class exposes a property to get the schema of serialization result of a graph.

    Schema generation also depends on `GraphSpec` , on which `serialize` serializes a graph.
    """
    def __init__(self, spec: 'GraphSpec', template: GraphTemplate, **serializers: 'NodeSerializer'):
        #: Specification of graph operations.
        self.spec = spec
        #: Graph template to serialize.
        self.template = template
        #: `NodeSerializer`s used for the serialization.
        self.serializers = serializers

    def _return_from(self, prop):
        ns = self.serializers[prop.name]

        arg = prop.kind

        # Generate a type holding GraphTemplate.
        # This type is ignored because serializer added by sub() resolve the type by iteself.
        arg = _templateType(prop.kind) if isinstance(prop.kind, GraphTemplate) else arg

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

    def schema_of(self, prop: GraphTemplate.Property) -> Type[Annotated]:
        """
        Generates structured and documented schema for a template property.

        Args:
            prop: A template property.
        Returns:
            Schema with documentation.
        """
        rt = self._return_from(prop)

        doc = self.serializers[prop.name]._doc or ""

        if issubclass(rt, dict):
            annotations = {}

            for c in filter(lambda c: c.name in self.serializers, prop.children):
                ns = self.serializers[c.name]
                cs = self.schema_of(c)

                t, d = _decompose_document(cs)

                if ns.be_merged:
                    if not issubclass(t, dict):
                        raise ValueError(f"Property '{c.name}' is not configured to be serialized into dict.")
                    annotations.update(**{ns.namer(k):t for k, t in get_annotated_hints(t).items()})
                else:
                    annotations[ns.namer(c.name)] = cs if ns.be_singular else document_type(List[t], d)

            return document_type(generate_schema(annotations, rt), doc)
        else:
            return document_type(rt, doc)

    @property
    def schema(self) -> TypedDict:
        """
        Generates `TypedDict` which represents the schema of serialized graph.
        """
        annotations = {}

        def put_root_schema(p):
            nonlocal annotations

            ns = self.serializers[p.name]
            dt = self.schema_of(p)

            if ns.be_merged:
                t, d = _decompose_document(dt)
                annotations.update(**{ns.namer(k):t_ for k, t_ in get_annotated_hints(t).items()})
            elif ns.be_singular:
                rt = signature(ns.aggregator).return_annotation
                if rt == Signature.empty or isinstance(rt, TypeVar):
                    rt = dt
                annotations[ns.namer(p.name)] = rt
            else:
                t, d = _decompose_document(dt)
                annotations[ns.namer(p.name)] = document_type(List[t], d)

        roots = filter(lambda p: p.parent is None and p.name in self.serializers, self.template._properties)

        for p in roots:
            put_root_schema(p)

        return generate_schema(annotations)

    def serialize(self, graph: 'GraphView', **node_params: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Serialize graph into a dictionary.

        Args:
            graph: A view of a graph.
            node_params: Parameters passed to `SerializationContext` and used by *serializer* s.
        Returns:
            Serialization result.
        """
        return self.spec.to_dict(graph, node_params, **self.serializers)
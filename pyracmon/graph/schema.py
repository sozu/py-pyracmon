import sys
from typing import get_type_hints, Generic, TypeVar
from inspect import signature, Signature
from .serialize import T


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
    if type(t) is type:
        return issubclass(t, p)
    else:
        return hasattr(t, '__origin__') and issubclass(get_origin(t), p)


class DocumentedType:
    def __init__(self, t, doc):
        self._type = t
        self._doc = doc


class Typeable(Generic[T]):
    pass


class DynamicType(Typeable[T]):
    @staticmethod
    def schema_of(dynamic, arg=None):
        bound = get_args(dynamic)[0]
        is_typevar = isinstance(bound, TypeVar)
        if arg is None and is_typevar:
            raise TypeError(f"Can't get schema of DynamicType whose type parameter is still TypeVar.")
        elif arg is not None and (not is_typevar and bound is not arg):
            raise TypeError(f"Bound type of DynamicType is {bound}, which is inconsistent with given type {arg}.")
        return dynamic.resolve(arg or bound)

    @classmethod
    def resolve(cls, bound):
        raise NotImplementedError()


class Shrink(Typeable[T]):
    @staticmethod
    def schema_of(shrink, arg=None):
        base = get_args(shrink)[0]

        if issubgeneric(base, Typeable):
            base = base.schema_of(base, arg)
        if not issubclass(base, TypedDict):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {base}.")

        return shrink.resolve(base, arg)

    @classmethod
    def resolve(cls, td, bound):
        raise NotImplementedError()


def extend_dict(td, **types):
    if '__annotations__' in td.__dict__:
        td.__annotations__.update(**types)
    else:
        setattr(td, '__annotations__', dict(**types))


def shrink_dict(td, *excludes):
    if '__annotations__' in td.__dict__:
        for k in excludes:
            if k in td.__annotations__:
                del td.__annotations__[k]


def walk_dict(td):
    if '__annotations__' not in td.__dict__:
        return {}

    result = {}

    for k, t in get_type_hints(td).items():
        t, conv = (t[0], lambda x:[x]) if isinstance(t, list) else (t, lambda x:x)

        if issubclass(t, TypedDict):
            result[k] = conv(walk_dict(t))
        else:
            result[k] = conv(t)
    
    return result


class GraphSchema:
    def __init__(self, spec, template, **serializers):
        self.spec = spec
        self.template = template
        self.serializers = serializers

    def _return_from(self, key):
        ns = self.serializers[key]

        rt = signature(ns.serializer).return_annotation

        if rt == Signature.empty:
            prop = getattr(self.template, key)
            if prop:
                f = self.spec.get_serializer(prop.kind)
                if f:
                    rt = signature(f).return_annotation

        return rt

    def schema_of(self, prop):
        rt = self._return_from(prop.name)

        if issubgeneric(rt, Typeable):
            rt = rt.schema_of(rt, prop.kind)
        elif rt == Signature.empty:
            rt = prop.kind

        if issubclass(rt, dict):
            class Schema(rt):
                pass

            anns = {}

            for c in filter(lambda c: c.name in self.serializers, prop.children):
                ns = self.serializers[c.name]
                cs = self.schema_of(c)

                if ns.be_merged:
                    if not issubclass(cs, dict):
                        raise ValueError(f"Property '{c.name}' is not configured to be serialized into dict.")
                    anns.update(**{ns.namer(k):t for k, t in get_type_hints(cs).items()})
                else:
                    anns[ns.namer(c.name)] = cs if ns.be_singular else [cs]

            setattr(Schema, '__annotations__', anns)

            return Schema
        else:
            return rt

    @property
    def schema(self):
        class Schema(TypedDict):
            pass

        def for_root(p):
            conv = (lambda x:x) if self.serializers[p.name].be_singular else (lambda x:[x])
            return conv(self.schema_of(p))

        roots = filter(lambda p: p.parent is None and p.name in self.serializers, self.template._properties)
        setattr(Schema, '__annotations__', {self.serializers[p.name].namer(p.name):for_root(p) for p in roots})

        return Schema

    def serialize(self, graph):
        self.spec.to_dict(graph, **self.serializers)
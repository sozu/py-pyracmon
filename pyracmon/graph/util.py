from functools import partial, reduce
from inspect import signature, Signature
from typing import TypeVar


T = TypeVar('T')


def as_is(x):
    return x


def wrap_serializer(f):
    try:
        sig = signature(f)
        def g(cxt, node, base, value) -> sig.return_annotation:
            ba = sig.bind(*(value, base, node, cxt)[len(sig.parameters)-1::-1])
            return f(*ba.args)
        return g
    except:
        def g(cxt, node, base, value):
            return f(value)
        return g


def chain_serializers(serializers):
    def merge(fs):
        rt = Signature.empty
        for f in fs[::-1]:
            t = signature(f).return_annotation
            if t != Signature.empty:
                try:
                    t[T]
                    rt = t if rt == Signature.empty else rt[t]
                except TypeError:
                    try:
                        return rt[t]
                    except TypeError:
                        return t
        return rt

    funcs = [wrap_serializer(f) for f in serializers]
    rt = merge(funcs)

    def composed(cxt, node, base, value) -> rt:
        base = [as_is, wrap_serializer(base)] if base else [as_is]
        return reduce(lambda acc,f: partial(f, cxt, node, acc), base + funcs)(value)

    return composed
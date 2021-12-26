from itertools import chain
from functools import partial, reduce
from inspect import signature, Signature
from typing import *


T = TypeVar('T')


# type aliases.
Serializer = Callable[['NodeContext'], Any]

TemplateProperty = Union[
    type,
    Tuple[type, Callable[[Any], Any]],
    Tuple[type, Callable[[Any], Any], Callable[[Any], bool]],
]


def chain_serializers(serializers: List[Serializer]) -> Serializer:
    """
    Creates a serializer which chains given serializers.

    Args:
        serializers: A list of serializers.
    Returns:
        Chained serializer.
    """
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

    rt = merge(serializers)
    def composed(cxt) -> rt:
        cxt._iterator = iter(serializers[::-1] + list(cxt._iterator))
        return cxt.serialize()

    return composed
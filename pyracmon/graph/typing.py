import sys
from functools import partial
from typing import Any, Dict, Optional, Type
import typing


try:
    import typing_extensions
except ImportError:
    typing_extensions = None


def _extended(*names):
    return all([hasattr(typing_extensions, n) for n in names])


__all__ = [
    "get_args",
    "get_origin",
    "TypedDict",
    "is_typed_dict",
    "Annotated",
    "annotate",
    "get_annotated_hints",
    "issubgeneric",
    "issubtype",
    "generate_schema",
]


_ge39 = sys.version_info[0:2] >= (3, 9)
_ge38 = sys.version_info[0:2] >= (3, 8)
_ge37 = sys.version_info[0:2] >= (3, 7)


# get_args, get_origin
if _ge38 or _extended('get_args', 'get_origin'):
    if _ge38:
        from typing import get_args, get_origin
    else:
        from typing_extensions import get_args, get_origin
elif _ge37:
    def get_args(tp):
        return getattr(tp, "__args__", [])
    def get_origin(tp):
        return getattr(tp, "__origin__", None)
else:
    def get_args(tp):
        def bind(base, args):
            if isinstance(args, tuple):
                t, subs = args
                return base[bind(t, subs)]
            else:
                return base[args]
        
        args = []
        for a in tp._subs_tree()[1:]:
            if isinstance(a, tuple):
                args.append(bind(a[0], a[1]))
            else:
                args.append(a)
        return args
    def get_origin(tp):
        return getattr(tp, "__origin__", None)


# TypedDict
if _ge38 or _extended('TypedDict', '_TypedDictMeta'):
    if _ge38:
        from typing import TypedDict, _TypedDictMeta
    else:
        from typing_extensions import TypedDict, _TypedDictMeta

    # In cpython <= 3.9, TypedDict has not yet supported subclass check by issubclass().
    def is_typed_dict(t: type) -> bool:
        return isinstance(t, _TypedDictMeta)
else:
    class TypedDict(dict):
        pass

    def is_typed_dict(t: type) -> bool:
        return issubclass(t, TypedDict)


# Annotated
if _ge39 or _extended('Annotated', 'get_type_hints'):
    if _ge39:
        from typing import Annotated, get_type_hints
    else:
        from typing_extensions import Annotated, get_type_hints

    get_annotated_hints = partial(get_type_hints, include_extras=True)

    def annotate(t: type, doc: str):
        if get_origin(t) == Annotated:
            return Annotated[get_args(t)[0], doc]
        else:
            return Annotated[t, doc]
else:
    from typing import TypeVar, Generic

    T = TypeVar('T')
    D = TypeVar('D')

    class Annotated(Generic[T, D]):
        pass

    def annotate(t: type, doc: str):
        class Doc:
            pass
        Doc.__doc__ = doc
        if get_origin(t) == Annotated:
            return Annotated[get_args(t)[0], Doc]
        else:
            return Annotated[t, Doc]

    def get_annotated_hints(t):
        hints = {}
        for u in t.__mro__:
            for k, v in getattr(u, "__annotations__", {}).items():
                if k not in hints:
                    hints[k] = v
        return hints


def issubgeneric(t: type, p: type) -> bool:
    """
    Checks whether a type is subclass of a generic type.

    Args:
        t: A type to check.
        p: A generic type.
    Returns:
        Whether the type in subclass of the generic type.
    """
    if type(t) is type:
        return issubclass(t, p)
    else:
        return hasattr(t, '__origin__') and issubclass(get_origin(t), p)


def issubtype(t: type, p: type) -> bool:
    """
    Checks whether a type is subclass of another type.

    Args:
        t: A type to check.
        p: Another type.
    Returns:
        Whether the type in subclass of another type.
    """
    if is_typed_dict(p):
        return is_typed_dict(t) and set(t.__annotations__.items()) >= set(p.__annotations__.items())
    else:
        return issubclass(t, p)


def generate_schema(annotations: Dict[str, Any], base: Optional[Type[TypedDict]] = None) -> Type[TypedDict]:
    """
    Generate schema as `TypedDict` by extending base schema.

    Args:
        annotations: Annotations to be set to new schema.
        base: Base schema if necessary.
    Returns:
        Generated schema.
    """
    class Schema(base or TypedDict):
        pass

    # In python3.6, "__annotations__" does not exist in "__dict__"
    # In python3.8, it exists even the class does not have any field.
    if "__annotations__" not in Schema.__dict__:
        setattr(Schema, "__annotations__", annotations)
    else:
        Schema.__annotations__.update(**annotations)

    return Schema

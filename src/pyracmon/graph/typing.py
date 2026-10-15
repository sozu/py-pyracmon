from collections.abc import Callable
from dataclasses import is_dataclass, fields
from inspect import Signature
from typing import Any, TypeVar, Literal, Optional, TypedDict, Annotated, Union, get_args, get_origin, get_type_hints, is_typeddict, overload
from types import UnionType


def issubgeneric(t: Any, p: type) -> bool:
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
        origin = get_origin(t)
        return isinstance(origin, type) and issubclass(origin, p)


def issubtype(t: type, p: type) -> bool:
    """
    Checks whether a type is subclass of another type.

    Args:
        t: A type to check.
        p: Another type.
    Returns:
        Whether the type in subclass of another type.
    """
    if is_typeddict(p):
        return is_typeddict(t) and set(t.__annotations__.items()) >= set(p.__annotations__.items())
    else:
        return issubclass(t, p)


def is_optional(t: Any) -> Any | None:
    """
    Checks if the given annotation corresponds to an optional type and returns the inner type.

    Args:
        t: Annotation value.
    Returns:
        Optional type if the annotation is optional, otherwise `None` .
    """
    org = get_origin(t)

    if org == Optional:
        return get_args(t)[0]
    elif org == Union or org == UnionType:
        args = get_args(t)
        return args[0] if len(args) == 2 and args[1] == type(None) else None
    else:
        return None


def replace_optional_typevar(t: Any, actual: Any) -> Any:
    """
    Replaces the first type variable in annotation value with actual type.

    This function is designed to deal with only optional types whose inner structure depends on python version and code style.

    Args:
        t: Annotation value.
        actual: Actual type to replace the type variable with.
    Returns:
        Annotation value with replaced type variable.
    """
    if t == Signature.empty or isinstance(t, TypeVar):
        return actual

    org = get_origin(t)
    args = get_args(t)

    def replace(gen: Any, targs: list[Any]) -> Any:
        var_found = False
        rargs: list[Any] = []

        for a in targs:
            if not var_found and isinstance(a, TypeVar):
                var_found = True
                rargs.append(replace_optional_typevar(a, actual))
            else:
                rargs.append(a)

        if get_origin(rargs[0]) == Annotated:
            # Move Annotated to the outermost.
            # Optional[Annotated[int, "ann"]] -> Annotated[Optional[int], "ann"]
            # Union[Annotated[int, "ann"], None] -> Annotated[Union[int, None], "ann"]
            ann_args = get_args(rargs[0])
            res = Annotated[replace(gen, [ann_args[0]] + rargs[1:]), ann_args[1]]
            for a in ann_args[2:]:
                res = Annotated[res, a]
        elif len(rargs) == 1:
            res = gen[rargs[0]]
        else:
            res = gen[rargs[0], rargs[1]]
            for a in rargs[2:]:
                res = gen[res, a]

        return res

    if org == Optional:
        return Optional[replace_optional_typevar(args[0], actual)]
    elif org == Union or org == UnionType:
        return replace(Union, list(args))
    elif org == Annotated:
        return replace(Annotated, list(args))
    else:
        return t


def to_typeddict(t: Any, strict: bool) -> type:
    """
    Convert an annotation `t` into `TypedDict` type.

    Args:
        t: Any kind of annotation.
        strict: If `True` , `TypeError` will be raised when `t` is neither a `TypeDict` nor a dataclass.
    Returns:
        `TypedDict` type which represents `t` .
    """
    if is_typeddict(t):
        return t
    elif is_dataclass(t):
        return TypedDict(t.__name__, {f.name:f.type for f in fields(t)}) # type: ignore
    else:
        if strict:
            raise TypeError(f"Type parameter must be resolved to TypedDict but {t}.")
        else:
            class TD(TypedDict):
                pass
            return TD


def to_rawdict(v: Any, strict: bool) -> dict:
    """
    Convert a value into builtin `dict` .

    Args:
        v: Any value.
        strict: If `True` , `TypeError` will be raised when `v` is an instance of neither a `dict` nor a dataclass.
    Returns:
        Converted `dict` .
    """
    if isinstance(v, dict):
        return v
    elif is_dataclass(v):
        return {f.name:getattr(v, f.name) for f in fields(v)}
    else:
        if strict:
            raise TypeError(f"The value must be a dict or dataclass instance but {type(v)}.")
        else:
            return {}


def generate_schema(annotations: dict[str, Any], base: type | None = None) -> type:
    """
    Generate schema as `TypedDict` by extending base schema.

    Args:
        annotations: Annotations to be set to new schema.
        base: Base schema if necessary.
    Returns:
        Generated schema.
    """
    # From python3.14, the structure of TypedDict is not changed by updating __annotations__.
    # Use inheritance to generate schema type instead.
    schema_type: type

    EXTRA = TypedDict('Extra', annotations) # type: ignore

    if base:
        class Schema(base, EXTRA):
            pass
        schema_type = Schema
    else:
        schema_type = EXTRA

    return schema_type


class Typeable[T]:
    """
    An interface for generic type which is resolved into a concrete type by a type parameter.

    Inherit this class and declare static method whose signature is `resolve(me, bound, arg, spec) -> type`.

    ```python
    >>> class A(Typeable[T]):
    >>>     @staticmethod
    >>>     def resolve(me, bound, arg, spec):
    >>>         ...
    >>>         return some_type
    >>>
    >>> Typeable.resolve(A[T], int, spec)
    ```

    Type resolution starts from `Typeable.resolve` which invokes the static method with following arguments.

    - Type to resolve itself, in this case, `A[T]`.
    - A resolved type which replace `T`.
        - `arg` is the first candidate.
        - When `arg` is also `Typeable` , this resolution flow is applied to it recursively until concrete type if determined.
    - `arg` is passed through as it is.
    - `spec` is passed through as it is.
    """
    @staticmethod
    def resolve(typeable, arg: Any, spec: Any) -> type:
        """
        Resolve a `Typeable` type into a concrete type by a type for its type parameter.

        Args:
            typeable: `Typeable` type having a generic type parameter.
            arg: Type to replace a type parameter.
            spec: An object containing information for schema generation.
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
    def is_resolved(typeable: type['Typeable']) -> bool:
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


class DynamicType[T](Typeable[T]):
    """
    A `Typeable` type which can be resolved dynamically with resolved type parameter.
    """
    @staticmethod
    def resolve(dynamic: type['DynamicType'], bound: type, arg: type, spec: Any) -> type:
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


class Shrink[T](Typeable[T]):
    """
    A type to remove keys from `TypedDict` bound to the type parameter `T`.

    This class only works when `TypedDict` parameter is set, otherwise `TypeError` is raised.
    """
    @staticmethod
    def resolve(shrink: type['Shrink'], bound: type | Signature, arg: type, spec: Any) -> type:
        """
        Resolve a `TypedDict` into another `TypedDict` by removing some keys defined by `select` .
        """
        if bound == Signature.empty:
            return TypedDict('_Shrink', {})

        bound = to_typeddict(bound, True)

        exc, inc = shrink.select(bound, arg)
        annotations = {n:t for n, t in get_type_hints(bound, include_extras=True).items() if (not inc or n in inc) and (n not in exc)}

        return generate_schema(annotations)

    @classmethod
    def select(cls, bound: type, arg: type) -> tuple[list[str], list[str]]:
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


class Extend[T](Typeable[T]):
    """
    A type to add keys to `TypedDict` bound to the type parameter `T`.

    This class only works when `TypedDict` parameter is set, otherwise `TypeError` is raised.
    """
    @staticmethod
    def resolve(extend: type['Extend'], bound: type | Signature, arg: type, spec: Any) -> type:
        """
        Resolve a `TypedDict` into another `TypedDict` by adding some keys retrieved by `schema` .
        """
        if bound == Signature.empty:
            return TypedDict('_Extend', {})

        bound = to_typeddict(bound, True)

        ext = extend.schema(bound, arg)
        td = to_typeddict(ext, False)

        return generate_schema(td.__annotations__, bound)

    @classmethod
    def schema(cls, bound: type, arg: type) -> Any:
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


def document_type(t: type, doc: str) -> Annotated:
    """
    Supplies a document to a type.

    Args:
        t: A type.
        doc: A document.
    Returns:
        Documented type.
    """
    return Annotated[t, doc]


def decompose_document(hint: Any) -> tuple[Any, str]:
    """
    Decomposes a type hint into a type-like object and a document.

    Args:
        hint: A type hint.
    Returns:
        A type-like object and a document.
    """
    if get_origin(hint) == Annotated:
        args = get_args(hint)
        # Annotated must have at least 2 arguments.
        # Last annotated string is used as document.
        t, d = args[0], args[-1]
        return t, d if isinstance(d, str) else d.__doc__
    else:
        return hint, ""


@overload
def walk_schema(td: Any, with_doc: Literal[False] = False) -> dict[str, type]:
    ...
@overload
def walk_schema(td: Any, with_doc: Literal[True] = True) -> dict[str, tuple[type, str]]:
    ...
def walk_schema(td: Any, with_doc = False):
    """
    Returns a dictionary as a result of walking a schema object from its root.

    Args:
        td: A schema represented by `TypedDict`.
        with_doc: Flag to include documentations into result.
    Returns:
        Key value representation of the schema. If `with_doc` is `True`, each value is `Annotated`.
    """
    result: dict[str, Any] = {}

    def put(k: str, t: type, doc: str):
        if with_doc:
            result[k] = (t, doc)
        else:
            result[k] = t

    def handle_type(t: Any) -> tuple[Any, Callable]:
        # Convert a type hint into a type-like object and a function to represent it in the result.
        return (get_args(t)[0], lambda x:[x]) if issubgeneric(t, list) else (t, lambda x:x)

    for k, hint in get_type_hints(td, include_extras=True).items():
        t, doc = decompose_document(hint)
        t, conv = handle_type(t)

        opt_type = is_optional(t)

        # TypedDict should be walked recursively to generate schema of nested objects.
        core_type = opt_type or t
        if is_typeddict(core_type):
            put(k, conv(walk_schema(core_type, with_doc)), doc)
        else:
            put(k, conv(t), doc)
    
    return result
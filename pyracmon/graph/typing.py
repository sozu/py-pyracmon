from inspect import Signature
from typing import Any, TypeVar, Generic, Optional, TypedDict, Annotated, Union, get_args, get_origin, is_typeddict, get_type_hints, cast


T = TypeVar('T')


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
        return origin is not None and issubclass(origin, p)


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


def generate_schema(annotations: dict[str, Any], base: Optional[type[TypedDict]] = None) -> type[TypedDict]:
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


class Typeable(Generic[T]):
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
    def resolve(typeable, arg: type, spec: Any) -> type:
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


class DynamicType(Typeable[T]):
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


class Shrink(Typeable[T]):
    """
    A type to remove keys from `TypedDict` bound to the type parameter `T`.

    This class only works when `TypedDict` parameter is set, otherwise `TypeError` is raised.
    """
    @staticmethod
    def resolve(shrink: type['Shrink'], bound: Union[type[TypedDict], Signature], arg: type, spec: Any) -> type:
        """
        Resolve a `TypedDict` into another `TypedDict` by removing some keys defined by `select` .
        """
        if bound == Signature.empty:
            return TypedDict
        if not is_typeddict(bound):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")
        bound = cast(type[TypedDict], bound)

        exc, inc = shrink.select(bound, arg)
        annotations = {n:t for n, t in get_type_hints(bound, include_extras=True).items() if (not inc or n in inc) and (n not in exc)}

        return generate_schema(annotations)

    @classmethod
    def select(cls, bound: type[TypedDict], arg: type) -> tuple[list[str], list[str]]:
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
    def resolve(extend: type['Extend'], bound: Union[type[TypedDict], Signature], arg: type, spec: Any) -> type:
        """
        Resolve a `TypedDict` into another `TypedDict` by adding some keys retrieved by `schema` .
        """
        if bound == Signature.empty:
            return TypedDict
        if not is_typeddict(bound):
            raise TypeError(f"Type parameter for Shrink must be resolved to TypedDict but {bound}.")
        bound = cast(type[TypedDict], bound)

        ext = extend.schema(bound, arg)

        return generate_schema(ext.__annotations__ if is_typeddict(ext) else {}, bound)

    @classmethod
    def schema(cls, bound: type[TypedDict], arg: type) -> Union[TypedDict, Signature.empty]:
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


def decompose_document(t: type) -> tuple[type, str]:
    if get_origin(t) == Annotated:
        args = get_args(t)
        # Annotated must have at least 2 arguments.
        # Last annotated string is used as document.
        t, d = args[0], args[-1]
        return t, d if isinstance(d, str) else d.__doc__
    else:
        return t, ""


def walk_schema(td, with_doc=False) -> dict[str, Union[type, Annotated]]:
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
        return (get_args(t)[0], lambda x:[x]) if issubgeneric(t, list) else (t, lambda x:x)

    for k, t in get_type_hints(td, include_extras=True).items():
        t, doc = decompose_document(t)

        t, conv = expand(t)

        if is_typeddict(t):
            put(k, conv(walk_schema(t, with_doc)), doc)
        else:
            put(k, conv(t), doc)
    
    return result
from typing import TypeVar, Generic, Protocol, Optional
from typing_extensions import Self
from collections.abc import Iterable, Mapping


KEY = TypeVar('KEY')


class NodeType(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def parents(self) -> Iterable[Self]: ...


N = TypeVar('N', bound=NodeType, covariant=True)


class NodePropType(NodeType, Protocol, Generic[N]):
    @property
    def children(self) -> Iterable[N]: ...


class MapNodeType(NodeType, Protocol, Generic[N, KEY]):
    @property
    def children(self) -> Mapping[KEY, Iterable[N]]: ...

    def __contains__(self, key: KEY) -> bool: ...


MN = TypeVar('MN', bound=MapNodeType, covariant=True)
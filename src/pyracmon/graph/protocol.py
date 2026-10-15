from typing import Protocol
from typing_extensions import Self
from collections.abc import Iterable, Mapping


class NodeType(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def parents(self) -> Iterable[Self]: ...


class NodePropType[N: NodeType](NodeType, Protocol):
    @property
    def children(self) -> Iterable[N]: ...


class MapNodeType[N: NodeType, KEY](NodeType, Protocol):
    @property
    def children(self) -> Mapping[KEY, Iterable[N]]: ...

    def __contains__(self, key: KEY) -> bool: ...
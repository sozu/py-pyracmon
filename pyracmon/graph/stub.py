"""
from collections.abc import Iterator
from typing import Any, Optional, Generic, TypeVar, ForwardRef, TYPE_CHECKING, cast
from typing_extensions import TypeAlias

from pyracmon.graph.graph import NodeView
from .graph import GraphView, ContainerView, Graph, NodeContainer
from .template import GraphTemplate


T = TypeVar('T')
GT = TypeVar('GT', bound=GraphTemplate)


class GraphTemplateView(Generic[GT], GraphView):
    def __call__(self) -> Graph: ...
    def __iter__(self) -> Iterator[tuple[str, ContainerView[NodeContainer]]]: ...
    def __getattr__(self, name: str) -> ContainerView: ...


class TypedProperty(Generic[T]):
    pass


TP = TypeVar('TP', bound=TypedProperty)


#class TypedNodeView(Generic[T], NodeView):
class TypedNodeView(Generic[T]):
    def __call__(self, alt: Optional[T] = None) -> T:
        ...
    def __getattr__(self, key: str) -> 'TypedContainerView':
        ...
    def __iter__(self) -> Iterator[tuple[str, 'TypedContainerView']]:
        ...


#class TypedContainerView(Generic[T], ContainerView):
class TypedContainerView(Generic[T]):
    def __call__(self) -> NodeContainer: ...
    def __bool__(self) -> bool: ...
    def __iter__(self) -> Iterator[TypedNodeView[T]]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: int) -> TypedNodeView[T]: ...


V = TypeVar('V', bound=GraphTemplateView)


class TypedGraphTemplate(Generic[V], GraphTemplate):
    pass


#-- stub
class MyTemplate(GraphTemplateView):
    class __a_node(TypedNodeView[int]):
        b: TypedContainerView[str]
    class __a_container(TypedContainerView[int]):
        b: TypedContainerView[str]
        def __iter__(self) -> Iterator['MyTemplate.__a_node']: ...
        def __getattr__(self, __name: str) -> TypedContainerView: ...
        def __getitem__(self, key: int) -> 'MyTemplate.__a_node': ...

    a: __a_container
    b: TypedContainerView[str]
#--


class TypedGraph(Generic[V], Graph):
    if TYPE_CHECKING:
        @property
        def view(self) -> V:
            ...


graph: TypedGraph[MyTemplate] = TypedGraph()
v = graph.view
aa = v.a[0]()
bb = v.a.b[0]()
"""
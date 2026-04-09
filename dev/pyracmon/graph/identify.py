from typing import Any, Callable, Optional
from collections.abc import Iterable, Mapping
from .protocol import *


class IdentifyPolicy:
    """
    Provides entity identification functionalities used during appending entities to a graph.

    Identification mechanism is based on the equality of identification keys extracted by entities.
    """
    def __init__(self, identifier: Optional[Callable[[Any], Any]]):
        #: A function to extract the identification key from an entity.
        self.identifier = identifier

    def get_identifier(self, value: Any) -> Any:
        """
        Returns identification key from an entity.

        Args:
            value: An entity.
        Returns:
            Identification key.
        """
        return self.identifier(value) if self.identifier else None

    def identify(
        self,
        prop: NodePropType,
        candidates: Iterable[MN],
        ancestors: Mapping[str, Iterable[MapNodeType[MapNodeType[MN, str], str]]],
    ) -> tuple[list[Optional[MN]], list[MN]]:
        """
        Select parent nodes and identical nodes of a new entity.

        This method is called during appending an entity to a graph.

        Args:
            prop: Template property for new entity.
            candidates: Nodes having the same identification key as the key of new entity.
            ancestors: Parent nodes mapped by property names.
        Returns
            The first item is a list of Parent nodes to which the node of new entity should be appended newly.
            `None` means to append a new node without parent. The second item is a list of identical nodes,
            which will be merged into ancestors and used in subsequent identifications of child entities.
        """
        raise NotImplementedError()


class HierarchicalPolicy(IdentifyPolicy):
    """
    Default identification policy used in a container where identification function is defined.

    This policy identifies nodes whose entity has the same identification key as the key of appending entity
    and whose parent is also identical to the parent of the entity.
    """
    def identify(
        self,
        prop: NodePropType,
        candidates: Iterable[MN],
        ancestors: Mapping[str, Iterable[MapNodeType[MapNodeType[MN, str], str]]],
    ):
        parents = sum((list(ancestors[p.name]) for p in prop.parents if p.name in ancestors), [])

        if parents:
            parent_nodes = []

            for pn in parents:
                # Find parent nodes which don't have child of the same identifier.
                if all([n not in pn.children[prop.name] for n in candidates]):
                    parent_nodes.append(pn)

            # Find identical nodes from candidates by checking whether the node belogs to a parent contained in ancestors.
            identical_nodes = [n for n in candidates if any([n in p.children[prop.name] for p in parents])]

            return parent_nodes, identical_nodes
        else:
            # This entity is a root entity. Key equality is only a criteria of identification.
            return ([], candidates) if candidates else ([None], [])


class NeverPolicy(IdentifyPolicy):
    """
    Identification policy which never identifies nodes.

    This policy is used in a container where identification function is not defined.
    """
    def identify(self, prop, candidates, ancestors):
        parents = sum((list(ancestors[p.name]) for p in prop.parents if p.name in ancestors), [])
        return (parents if parents else [None]), []


def neverPolicy(instance=NeverPolicy(None)) -> IdentifyPolicy:
    return instance


class AlwaysPolicy(IdentifyPolicy):
    def identify(self, prop, candidates, ancestors):
        parents = sum((list(ancestors[p.name]) for p in prop.parents if p.name in ancestors), [])

        if parents:
            parent_nodes = []

            for p in parents:
                # Find parent nodes which don't have child of the same identifier.
                if all([n not in p.children[prop.name] for n in candidates]):
                    parent_nodes.append(p)

            return parent_nodes, candidates
        else:
            return ([], candidates) if candidates else ([None], [])
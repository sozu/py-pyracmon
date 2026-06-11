from typing import Any, Callable
from collections.abc import Iterable, Mapping
from .protocol import NodePropType, MapNodeType


class IdentifyPolicy:
    """
    Provides entity identification functionality used while appending entities to a graph.

    The identification mechanism is based on the equality of identification keys extracted from entities.
    """
    def __init__(self, identifier: Callable[[Any], Any] | None):
        #: A function to extract the identification key from an entity.
        self.identifier = identifier

    def get_identifier(self, value: Any) -> Any:
        """
        Returns the identification key for the given entity.

        Args:
            value: The entity to identify.
        Returns:
            The identification key, or `None` if `value` is `None` or no identifier function is configured.
        """
        return self.identifier(value) if self.identifier and value is not None else None

    def identify[MN: MapNodeType](
        self,
        prop: NodePropType,
        candidates: Iterable[MN],
        ancestors: Mapping[str, Iterable[MapNodeType[MapNodeType[MN, str], str]]],
    ) -> tuple[list[MN | None], list[MN]]:
        """
        Selects the parent nodes and identical nodes for a new entity.

        - Parent nodes:
            - Nodes to which the node of the new entity should be appended.
            - When the list contains `None`, it means that the node of the new entity, which has no parent, should also be appended.
        - Identical nodes:
            - Nodes that are assumed to hold an **identical** entity, and should therefore be reused as the node for the new entity.
            - The new node is created under parent nodes that don't have an identical node.

        This method is called while an entity is being appended to a graph.

        Args:
            prop: The template property of the new entity.
            candidates: The nodes that can be identical nodes, because they have the same identification key as the new entity.
            ancestors: The parent nodes, mapped by property name, to which the parent entities were appended during this operation.
        Returns:
            A tuple of the parent nodes and the identical nodes.
        """
        raise NotImplementedError()


class HierarchicalPolicy(IdentifyPolicy):
    """
    An identification policy that depends on identification keys and parent-child relationships.

    - Parent nodes are selected from the ancestors if they don't have a child with the same identification key.
    - Identical nodes are selected from the candidates if they belong to a parent contained in the ancestors.

    This policy is designed to handle the result of a query that joins multiple tables.
    """
    def identify[MN: MapNodeType](
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

            # Find identical nodes from candidates by checking whether the node belongs to a parent contained in ancestors.
            identical_nodes = [n for n in candidates if any([n in p.children[prop.name] for p in parents])]

            return parent_nodes, identical_nodes
        else:
            # This entity is a root entity. Key equality is only a criteria of identification.
            return ([], candidates) if candidates else ([None], [])


class NeverPolicy(IdentifyPolicy):
    """
    An identification policy that never identifies nodes.

    This policy is used in a container where no identification function is defined.
    """
    def identify(self, prop, candidates, ancestors):
        parents = sum((list(ancestors[p.name]) for p in prop.parents if p.name in ancestors), [])
        return (parents if parents else [None]), []


def neverPolicy(instance=NeverPolicy(None)) -> IdentifyPolicy:
    return instance


class AlwaysPolicy(IdentifyPolicy):
    """
    Similar to `HierarchicalPolicy`, but identical nodes are selected only by key equality, without checking parent-child relationships.
    """
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
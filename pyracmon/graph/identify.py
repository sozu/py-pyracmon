from typing import *


class IdentifyPolicy:
    """
    Provides entity identification functionalities used during appending entities to a graph.

    Identification mechanism is based on the equality of identification keys extracted by entities.
    """
    def __init__(self, identifier: Callable[[Any], Any]):
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
        prop: 'GraphTemplate.Property',
        candidates: List['Node'],
        ancestors: Dict[str, List['Node']],
    ) -> Tuple[List[Optional['Node']], List['Node']]:
        """
        Select parent nodes and identical nodes of a new entity.

        This method is called during appending an entity to a graph.

        Args:
            prop: Template property for new entity.
            candidates: Nodes having the same identification key as the key of new entity.
            ancestors: Parent nodes mapped by property names.
        Returns
            The first item is a list of Parent nodes which the node of new entity should be appended newly.
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
    def identify(self, prop, candidates, ancestors):
        if prop.parent and prop.parent.name in ancestors:
            # This entity has parent in this session.
            parents = ancestors[prop.parent.name]

            parent_nodes = []

            for p in parents:
                # Find parent nodes which don't have child of the same identifier.
                if all([not p.children[prop.name].has(n) for n in candidates]):
                    parent_nodes.append(p)

            # Find identical nodes from candidates by checking whether the node belogs to a parent contained in ancestors.
            identical_nodes = [n for n in candidates if any([p.children[prop.name].has(n) for p in parents])]

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
        if prop.parent and prop.parent.name in ancestors:
            return ancestors[prop.parent.name], []
        else:
            return [None], []


def neverPolicy(instance=NeverPolicy(None)):
    return instance


class AlwaysPolicy(IdentifyPolicy):
    def identify(self, prop, candidates, ancestors):
        if prop.parent and prop.parent.name in ancestors:
            parents = ancestors[prop.parent.name]

            parent_nodes = []

            for p in parents:
                # Find parent nodes which don't have child of the same identifier.
                if all([not p.children[prop.name].has(n) for n in candidates]):
                    parent_nodes.append(p)

            return parent_nodes, candidates
        else:
            return ([], candidates) if candidates else ([None], [])
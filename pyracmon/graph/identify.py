class IdentifyPolicy:
    """
    Provides functionalities to handle identical entities in appending sessions of a graph.
    """
    def __init__(self, identifier):
        self.identifier = identifier

    def get_identifier(self, value):
        """
        Returns identification key from an entity value.

        Parameters
        ----------
        value: object
            An entity value.

        Returns
        -------
        object
            Identification key of the entity value.
        """
        return self.identifier(value) if self.identifier else None

    def identify(self, prop, candidates, ancestors):
        """
        Select parent nodes and identical nodes of a new entity.

        Parameters
        ----------
        prop: GraphTemplate.Property
            Template property for new entity.
        candidates: [Node]
            Nodes having the same identification key.
        ancestors: {str: [Node]}
            Mappings from property name to identical node set which appeared in current appending session.

        Returns
        -------
        [Node | None]
            Parent nodes which the node of new entity should be appended newly.
            `None` means to append a new node without parent.
        [Node]
            Identical existing nodes propagated to identifications of children as ancestors.
        """
        raise NotImplementedError()


class HierarchicalPolicy(IdentifyPolicy):
    """
    Default policy for template properties having identifier.

    This policy identifies nodes whose entity has the same identification key as one of appending entity
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
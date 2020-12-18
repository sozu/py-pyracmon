class IdentifyPolicy:
    def __init__(self, identifier):
        self.identifier = identifier

    def identify(self, prop, candidates, ancestors):
        """
        Select parent nodes and identical nodes of a new entity.

        Parameters
        ----------
        prop: GraphTemplate.Property
            Template property for new entity.
        candidate: [Node]
            Nodes having the same identifier.
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
    def identify(self, prop, candidates, ancestors):
        #candidates = get_nodes(self.identifier(entity))

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
        #candidates = get_nodes(self.identifier(entity))

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
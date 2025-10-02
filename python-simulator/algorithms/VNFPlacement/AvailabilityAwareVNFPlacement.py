class AvailabilityAwareVNFPlacement:
    def __init__(
        self,
        system,
    ):
        self.system = system
        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.get_node_score(node),
        )

    def placement(self):
        for sfc in self.system.sfcs:
            for vnf in sfc.vnfs:
                placed = False if vnf.node is None else True

                while self.candidate_nodes and not placed:
                    node = self.candidate_nodes[0]
                    success = self.system.vnf_placement(vnf, node, sfc)

                    if success:
                        placed = True
                    else:
                        self.candidate_nodes.pop(0)

                if not placed:
                    return False

            for virtual_link in sfc.virtual_links:
                path = self.system.get_candidate_path(
                    virtual_link.source.node, virtual_link.target.node
                )

                if path:
                    self.system.virtual_link_mapping(virtual_link, path, sfc)

        return self.system

    def get_node_score(self, candidate_node):
        return - candidate_node.availability

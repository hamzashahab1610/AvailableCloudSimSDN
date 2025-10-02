import math


class TradeoffAwareVNFPlacement:
    def __init__(
        self,
        system,
    ):
        self.system = system

        # Find min and max values for normalization
        self.min_availability = min(node.availability for node in self.system.nodes)
        self.max_availability = max(node.availability for node in self.system.nodes)
        self.min_carbon_footprint = min(node.carbon_footprint for node in self.system.nodes)
        self.max_carbon_footprint = max(node.carbon_footprint for node in self.system.nodes)

        self.candidate_nodes = sorted(
            self.system.nodes,
            key=lambda node: self.get_node_score(node),
        )

    def placement(self):
        for sfc in self.system.sfcs:
            for vnf in sfc.vnfs:
                placed = False if vnf.node is None else True

                while self.candidate_nodes and not placed:
                    node = self.candidate_nodes[0]  # Try best candidate first
                    success = self.system.vnf_placement(vnf, node, sfc)

                    if success:
                        placed = True
                    else:
                        # Remove failed node and try next best
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
         # Min-max normalization
        normalized_availability = 0
        if self.max_availability > self.min_availability:
            normalized_availability = (candidate_node.availability - self.min_availability) / (self.max_availability - self.min_availability)
        
        normalized_carbon_footprint = 0
        if self.max_carbon_footprint > self.min_carbon_footprint:
            normalized_carbon_footprint = (candidate_node.carbon_footprint - self.min_carbon_footprint) / (self.max_carbon_footprint - self.min_carbon_footprint)

        score = normalized_availability - normalized_carbon_footprint

        return -score

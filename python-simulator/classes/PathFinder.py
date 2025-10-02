import networkx as nx
from itertools import islice
from classes import Path, Channel, Switch


class PathFinder:
    def __init__(self, graph, switches):
        self.G = graph
        self.switches = switches
        self.path_cache = {}

    def get_path(self, source, target):
        cache_key = (source.name, target.name)

        if cache_key in self.path_cache:
            return self.path_cache[cache_key]

        if source == target:
            return self._create_self_path(source)

        # Compute new path
        path = self._compute_path(source, target)
        self.path_cache[cache_key] = path
        return path

    def _compute_path(self, source, target):
        shortest_paths = list(
            islice(nx.shortest_simple_paths(self.G, source.name, target.name), 3)
        )
        node_paths = shortest_paths

        # print(f"Found {len(node_paths)} paths from {source.name} to {target.name}")

        path_channels = []
        for path in node_paths:
            switches = []

            # Extract switches
            for i in range(len(path) - 1):
                curr = path[i]

                if "Switch" in str(curr):
                    switch = self._find_switch(curr)
                    switches.append(switch)

            channel = Channel.Channel(
                name=f"{[n for n in path]}",
                source=source,
                target=target,
                switches=switches,
                time_active=24,
            )
            path_channels.append(channel)

        return Path.Path(
            name=f"{source.name}->{target.name}",
            source=source,
            target=target,
            channels=path_channels,
        )

    def _create_self_path(self, node):
        """Create path from node to itself"""
        internal_switch = Switch.Switch(
            name="internal",
            type="Switch",
            bandwidth=10000000,
            availability=1,
            p_static=0,
            p_port=0,
            co2=0,
            processing_delay=0,
        )

        channel = Channel.Channel(
            name=f"[{node.name}]",
            source=node,
            target=node,
            switches=[internal_switch],
            time_active=24,
        )

        return Path.Path(
            name=f"{node.name}->{node.name}",
            source=node,
            target=node,
            channels=[channel],
        )

    def _find_switch(self, name):
        for switch in self.switches:
            if switch.name == name:
                return switch
        return None

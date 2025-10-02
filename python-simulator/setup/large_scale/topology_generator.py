import sys, os, json

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from classes import Node, Switch, PhysicalLink
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# nodes_df = pd.read_excel(
#     "setup/large_scale/Mapping.xlsx",
#     sheet_name="Trace Log Machines",
#     header=0,
#     nrows=90883,
# )
nodes_df = pd.read_excel(
    "setup/large_scale/Mapping.xlsx",
    sheet_name="Trace Log Machines - Edge",
    header=0,
    nrows=25338,
)
# nodes_df = pd.read_excel(
#     "setup/large_scale/Mapping.xlsx",
#     sheet_name="Trace Log Machines - Cloud",
#     header=0,
#     nrows=65545,
# )

availability_power_profiles = pd.read_excel(
    "setup/large_scale/Mapping.xlsx",
    sheet_name="Availability Power Mappings",
    header=0,
    nrows=11,
)

node_profiles = pd.read_excel(
    "setup/large_scale/Mapping.xlsx",
    sheet_name="Machine Mappings",
    header=1,
    nrows=18,
)


def create_node(index, row):
    # Find matching profile for node based on platform_id, cpu_capacity, memory_capacity
    availability_power_profile = availability_power_profiles.loc[
        (round(availability_power_profiles["availability"],2) == round(row["availability"], 2))
    ].to_dict(orient="records")

    node_profile = node_profiles.loc[
        (node_profiles["platform_id"] == row["platform_id"])
        & (round(node_profiles["normalized_cpu_capacity"],3) == round(row["cpu_capacity"],3))
        & (round(node_profiles["normalized_memory_capacity"],3) == round(row["memory_capacity"],3))
    ].to_dict(orient="records")

    if availability_power_profile and node_profile:
        print(f"Creating node {index + 1}/{len(nodes_df)}: {row['machine_id']}")
        
        return Node.Node(
            name=row["machine_id"],
            platform=row["platform_id"],
            cpu=node_profile[0]["cpu_capacity"],
            ram=node_profile[0]["memory_capacity"] * 1024,
            storage=node_profile[0]["storage_capacity"] * 1024,
            mtbf=row["mtbf"],
            mttr=row["mttr"],
            availability=row["availability"],
            p_min=availability_power_profile[0]["p_min"],
            p_max=availability_power_profile[0]["p_max"],
            co2=0.35,  # Optimized Energy Cost and Carbon Emission-Aware Virtual Machine Allocation in Sustainable Data Centers; Energy and Carbon-Efficient Placement of Virtual Machines in Distributed Cloud Data Centers
            processing_delay=1,  # Default processing delay,
            time_active=24,
        )
    else:
        raise ValueError(f"No matching profile found for node {row['machine_id']}")


def validate_k(k):
    """Validate that k is a positive even number."""
    if not isinstance(k, int) or k <= 0 or k % 2 != 0:
        raise ValueError("k must be a positive even number")


def generate_fat_tree(k):
    """Generate complete fat tree topology with all nodes first."""
    validate_k(k)
    G = nx.Graph()
    physical_links = []

    # 1. Create all nodes first
    nodes = []

    for index, row in nodes_df.iterrows():
        node = create_node(index, row)
        nodes.append(node)

    # edge_nodes = []
    # cloud_nodes = []

    # for index, row in nodes_df.iterrows():
    #     node = create_node(index, row)
    #     nodes.append(node)
    #     if node.cpu == 8:
    #         edge_nodes.append(node)
    #     elif node.cpu >= 16:
    #         cloud_nodes.append(node)

    # total_nodes = len(nodes)
    # total_edge_nodes = len(edge_nodes)
    # total_cloud_nodes = len(cloud_nodes)

    # edge_percentage = (total_edge_nodes / total_nodes) * 100
    # cloud_percentage = (total_cloud_nodes / total_nodes) * 100

    # print(f"Total nodes: {total_nodes}")
    # print(f"Edge nodes: {total_edge_nodes} ({edge_percentage:.2f}%)")
    # print(f"Cloud nodes: {total_cloud_nodes} ({cloud_percentage:.2f}%)")

    # 2. Create all switches
    num_pods = k

    # Create core switches (k/2)^2 total core switches
    core_switches = []
    for i in range(k // 2):
        for j in range(k // 2):
            print(
                f"Creating core switch {i * (k // 2) + j + 1}/{(k // 2) ** 2}: CoreSwitch_{i}_{j}"
            )
            core_switches.append(
                Switch.Switch(
                    name=f"CoreSwitch_{i}_{j}",
                    type="Switch",
                    bandwidth=500
                    * 1000000,  # Scalability and Performance Evaluation of Edge Cloud Systems for Latency Constrained Applications
                    availability=0.99999,  # An Availability-aware SFC placement Algorithm for Fat-Tree Data Centers
                    p_static=300,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                    p_port=0.0005,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                    co2=0.35,  # Optimized Energy Cost and Carbon Emission-Aware Virtual Machine Allocation in Sustainable Data Centers; Energy and Carbon-Efficient Placement of Virtual Machines in Distributed Cloud Data Centers
                    processing_delay=100,  # Latency-aware Virtualized Network Function provisioning for distributed edge clouds
                )
            )

    aggr_switches = []
    for i in range(num_pods * (k // 2)):
        print(
            f"Creating aggregation switch {i + 1}/{num_pods * (k // 2)}: AggrSwitch_pod{i // k}_sw{i % k}"
        )
        aggr_switches.append(
            Switch.Switch(
                name=f"AggrSwitch_pod{i // k}_sw{i % k}",
                type="Switch",
                bandwidth=100
                * 1000000,  # Scalability and Performance Evaluation of Edge Cloud Systems for Latency Constrained Applications
                availability=0.9999,  # An Availability-aware SFC placement Algorithm for Fat-Tree Data Centers
                p_static=150,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                p_port=0.0005,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                co2=0.35,  # Optimized Energy Cost and Carbon Emission-Aware Virtual Machine Allocation in Sustainable Data Centers; Energy and Carbon-Efficient Placement of Virtual Machines in Distributed Cloud Data Centers
                processing_delay=1,  # Latency-aware Virtualized Network Function provisioning for distributed edge clouds
            )
        )

    edge_switches = []
    for i in range(num_pods * (k // 2)):
        print(
            f"Creating edge switch {i + 1}/{num_pods * (k // 2)}: EdgeSwitch_pod{i // k}_sw{i % k}"
        )
        edge_switches.append(
            Switch.Switch(
                name=f"EdgeSwitch_pod{i // k}_sw{i % k}",
                type="Switch",
                bandwidth=100
                * 1000000,  # Scalability and Performance Evaluation of Edge Cloud Systems for Latency Constrained Applications
                availability=0.9999,  # An Availability-aware SFC placement Algorithm for Fat-Tree Data Centers
                p_static=150,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                p_port=0.0005,  # Power Minimization in Fat-Tree SDN Datacenter Operation
                co2=0.35,  # Optimized Energy Cost and Carbon Emission-Aware Virtual Machine Allocation in Sustainable Data Centers; Energy and Carbon-Efficient Placement of Virtual Machines in Distributed Cloud Data Centers
                processing_delay=1,  # Latency-aware Virtualized Network Function provisioning for distributed edge clouds
            )
        )

    # 3. Add all nodes and switches to graph
    G.add_nodes_from([switch.name for switch in core_switches], layer=3)
    G.add_nodes_from([switch.name for switch in aggr_switches], layer=2)
    G.add_nodes_from([switch.name for switch in edge_switches], layer=1)
    G.add_nodes_from([node.name for node in nodes], layer=0)

    # 4. Connect nodes to edge switches ((k/2) nodes per edge switch)
    nodes_per_edge = k // 2
    for i, edge_switch in enumerate(edge_switches):
        node_start = i * nodes_per_edge
        node_end = min((i + 1) * nodes_per_edge, len(nodes))
        for node in nodes[node_start:node_end]:
            G.add_edge(edge_switch.name, node.name)

            print(f"Creating link between {edge_switch.name} and {node.name}")

            physical_links.append(
                PhysicalLink.PhysicalLink(
                    name=f"Link_{edge_switch.name}_{node.name}",
                    source=edge_switch,
                    target=node,
                )
            )

    # 5. Connect edge switches to aggregation switches within pods
    for pod in range(num_pods):
        pod_aggr = aggr_switches[pod * (k // 2) : (pod + 1) * (k // 2)]
        pod_edge = edge_switches[pod * (k // 2) : (pod + 1) * (k // 2)]

        # Full mesh between edge and aggregation within pod
        for edge in pod_edge:
            for aggr in pod_aggr:
                G.add_edge(edge.name, aggr.name)

                print(f"Creating link between {edge.name} and {aggr.name}")

                physical_links.append(
                    PhysicalLink.PhysicalLink(
                        name=f"Link_{edge.name}_{aggr.name}",
                        source=edge,
                        target=aggr,
                    )
                )

    # 6. Connect aggregation switches to core switches
    # In each pod, the i-th aggregation switch connects to the i-th core switch in each group
    for pod in range(k):
        for aggr_id in range(k // 2):  # Position within pod
            aggr_switch = aggr_switches[pod * (k // 2) + aggr_id]

            # Connect to k/2 core switches
            for j in range(k // 2):
                # Core switch index: aggr_id*(k/2) + j
                core_switch = core_switches[aggr_id * (k // 2) + j]

                G.add_edge(aggr_switch.name, core_switch.name)

                print(
                    f"Creating link between {aggr_switch.name} and {core_switch.name}"
                )

                physical_links.append(
                    PhysicalLink.PhysicalLink(
                        name=f"Link_{aggr_switch.name}_{core_switch.name}",
                        source=aggr_switch,
                        target=core_switch,
                    )
                )

    return G, nodes, edge_switches, aggr_switches, core_switches, physical_links


def convert_topology_to_json(
    nodes, edge_switches, aggr_switches, core_switches, physical_links
):
    topology = {
        "datacenters": [
            {"name": "dc1"},
        ],
        "nodes": [],
        "links": [],
    }

    # Add nodes
    for node in nodes:
        node_json = {
            "name": str(node.name),
            "type": "host",
            "pes": node.cpu,
            "mips": node.cpu * 1000000,  # Convert CPU to MIPS
            "ram": node.ram,
            "storage": node.storage,
            "bw": 1000000000,  # Using platform as bandwidth multiplier
            "datacenter": "dc1",
            "availability": node.initial_availability,
            "p_min": node.p_min,
            "p_max": node.p_max,
            "co2": node.co2,
        }
        topology["nodes"].append(node_json)

    # Add switches
    for switch in core_switches:
        switch_json = {
            "name": switch.name,
            "type": "core",
            "iops": switch.bandwidth,
            "upports": 0,
            "downports": len(
                [link for link in physical_links if link.source == switch]
            ),
            "bw": switch.bandwidth,
            "datacenter": "dc1",
            "availability": switch.availability,
            "p_static": switch.p_static,
            "p_port": switch.p_port,
            "co2": switch.co2,
        }
        topology["nodes"].append(switch_json)

    for switch in edge_switches:
        switch_json = {
            "name": switch.name,
            "type": "edge",
            "iops": switch.bandwidth,
            "upports": 1,
            "downports": len(
                [link for link in physical_links if link.source == switch]
            ),
            "bw": switch.bandwidth,
            "datacenter": "dc1",
            "availability": switch.availability,
            "p_static": switch.p_static,
            "p_port": switch.p_port,
            "co2": switch.co2,
        }
        topology["nodes"].append(switch_json)

    for switch in aggr_switches:
        switch_json = {
            "name": switch.name,
            "type": "aggregate",
            "iops": switch.bandwidth,
            "upports": 1,
            "downports": len(
                [link for link in physical_links if link.source == switch]
            ),
            "bw": switch.bandwidth,
            "datacenter": "dc1",
            "availability": switch.availability,
            "p_static": switch.p_static,
            "p_port": switch.p_port,
            "co2": switch.co2,
        }
        topology["nodes"].append(switch_json)

    # Add links
    for link in physical_links:
        link_json = {
            "source": str(link.source.name),
            "destination": str(link.target.name),
            "latency": float(
                link.source.processing_delay + link.target.processing_delay
            ),
        }
        topology["links"].append(link_json)

    return topology

def create_topology_from_system(nodes, switches, physical_links):
    # Create new graph
    G = nx.Graph()

    # Categorize switches by their type
    core_switches = [sw for sw in switches if sw.name in ["s13", "s14"]]
    aggr_switches = [
        sw for sw in switches if sw.name in ["s7", "s8", "s9", "s10", "s11", "s12"]
    ]
    edge_switches = [
        sw for sw in switches if sw.name in ["s1", "s2", "s3", "s4", "s5", "s6"]
    ]

    # Add nodes to graph with their respective layers
    G.add_nodes_from([sw.name for sw in core_switches], layer=3)
    G.add_nodes_from([sw.name for sw in aggr_switches], layer=2)
    G.add_nodes_from([sw.name for sw in edge_switches], layer=1)
    G.add_nodes_from([node.name for node in nodes], layer=0)

    # Add edges from physical links
    for link in physical_links:
        G.add_edge(link.source.name, link.target.name)

    # Create topology dictionary similar to create_reduced_topology_from_system
    topology = {"datacenters": [{"name": "dc1"}], "nodes": [], "links": []}

    # Add host nodes to topology
    for node in nodes:
        topology["nodes"].append({
            "name": str(node.name),
            "type": "host",
            "pes": node.total_cpu,
            "mips": node.total_cpu * 1000000,  # Convert CPU to MIPS
            "ram": node.total_ram,
            "storage": node.total_storage,
            "bw": 1000000000,  # Using platform as bandwidth multiplier
            "datacenter": "dc1",
            "mtbf": node.mtbf,
            "mttr": node.mttr,
            "availability": node.initial_availability,
            "p_min": node.p_min,
            "p_max": node.p_max,
            "co2": node.co2,
        })

    # Add switches to topology
    for switch in switches:
        # Determine switch type based on categorization
        switch_type = "core" if switch in core_switches else \
                     "aggregate" if switch in aggr_switches else "edge"
        
        topology["nodes"].append({
            "name": switch.name,
            "type": switch_type,
            "iops": switch.total_bandwidth,
            "upports": 0 if switch_type == "core" else 1,
            "downports": len([link for link in physical_links 
                              if link.source == switch or link.target == switch]),
            "bw": switch.total_bandwidth,
            "datacenter": "dc1",
            "availability": switch.availability,
            "p_static": switch.p_static,
            "p_port": switch.p_port,
            "co2": switch.co2,
        })

    # Add links to topology
    for link in physical_links:
        topology["links"].append({
            "source": str(link.source.name),
            "destination": str(link.target.name),
            "latency": float(link.source.processing_delay + link.target.processing_delay),
        })

    return G, topology

def create_reduced_topology_from_system(system, spare_hosts_percentage=0.3):
    G = nx.Graph()

    used_hosts = [node for node in system.nodes if len(node.vnfs) > 0]
    print(f"Found {len(used_hosts)} active hosts")

    num_spare_hosts = int(len(used_hosts) * spare_hosts_percentage)

    potential_spare_hosts = [
        node for node in system.candidate_nodes if node not in used_hosts
    ]

    spare_hosts = potential_spare_hosts[:num_spare_hosts]
    print(
        f"Added {len(spare_hosts)} spare hosts from candidate nodes for backup placement"
    )

    all_hosts = spare_hosts + used_hosts

    edge_switches = []
    print("Finding edge switches connected to active hosts...")
    for link in system.physical_links:
        if link.source in all_hosts or link.target in all_hosts:
            if link.source.type == "Switch" and link.source not in edge_switches:
                edge_switches.append(link.source)
                print(f"Added edge switch: {link.source.name}")

    aggr_switches = []
    print("Finding aggregation switches connected to edge switches...")
    for edge_switch in edge_switches:
        for link in system.physical_links:
            if (
                link.source.name == edge_switch.name
                and link.target.type == "Switch"
                and link.target not in aggr_switches
            ):
                aggr_switches.append(link.target)
                print(f"Added aggregation switch: {link.target.name}")

    core_switches = []
    print("Finding core switches connected to aggregation switches...")
    for aggr_switch in aggr_switches:
        for link in system.physical_links:
            if (
                link.source.name == aggr_switch.name
                and link.target.type == "Switch"
                and link.target not in core_switches
            ):
                core_switches.append(link.target)
                print(f"Added core switch: {link.target.name}")

    used_switches = edge_switches + aggr_switches + core_switches
    print(
        f"Total switches in reduced topology: {len(used_switches)} "
        + f"(Edge: {len(edge_switches)}, Aggr: {len(aggr_switches)}, Core: {len(core_switches)})"
    )

    used_links = [
        link
        for link in system.physical_links
        if link.source in all_hosts + used_switches
        and link.target in all_hosts + used_switches
    ]

    G.add_nodes_from([sw.name for sw in used_switches if "Core" in sw.name], layer=3)
    G.add_nodes_from([sw.name for sw in used_switches if "Aggr" in sw.name], layer=2)
    G.add_nodes_from([sw.name for sw in used_switches if "Edge" in sw.name], layer=1)
    G.add_nodes_from([node.name for node in all_hosts], layer=0)

    for link in used_links:
        G.add_edge(link.source.name, link.target.name)

    topology = {"datacenters": [{"name": "dc1"}], "nodes": [], "links": []}

    for node in all_hosts:
        topology["nodes"].append(
            {
                "name": str(node.name),
                "type": "host",
                "pes": node.total_cpu,
                "mips": node.total_cpu * 1000000,  # Convert CPU to MIPS
                "ram": node.total_ram,
                "storage": node.total_storage,
                "bw": 1000000000,  # Using platform as bandwidth multiplier
                "datacenter": "dc1",
                "mtbf": node.mtbf,
                "mttr": node.mttr,
                "availability": node.initial_availability,
                "p_min": node.p_min,
                "p_max": node.p_max,
                "co2": node.co2,
            }
        )

    for switch in used_switches:
        topology["nodes"].append(
            {
                "name": switch.name,
                "type": (
                    "core"
                    if "Core" in switch.name
                    else "aggregate" if "Aggr" in switch.name else "edge"
                ),
                "iops": switch.total_bandwidth,
                "upports": 1,
                "downports": 4,
                "bw": switch.total_bandwidth,
                "datacenter": "dc1",
                "availability": switch.availability,
                "p_static": switch.p_static,
                "p_port": switch.p_port,
                "co2": switch.co2,
            }
        )

    for link in used_links:
        topology["links"].append(
            {
                "source": str(link.source.name),
                "destination": str(link.target.name),
                "latency": float(
                    link.source.processing_delay + link.target.processing_delay
                ),
            }
        )

    return G, topology

def visualize_fat_tree(G, save_path=None):
    plt.figure(figsize=(12, 8))
    pos = nx.multipartite_layout(G, subset_key="layer", align="horizontal")

    nx.draw_networkx_nodes(G, pos, node_color="lightblue", node_size=500)
    nx.draw_networkx_edges(G, pos)

    # labels = {node: node.name for node in G.nodes()}
    nx.draw_networkx_labels(G, pos)

    plt.title(f"Fat Tree Topology")

    if save_path:
        plt.savefig(save_path)

    plt.show()


def visualize_reduced_topology(G, save_path="reduced_topology.png"):
    plt.figure(figsize=(12, 8))
    pos = nx.multipartite_layout(G, subset_key="layer", align="horizontal")

    nx.draw_networkx_nodes(G, pos, node_color="lightblue", node_size=500)
    nx.draw_networkx_edges(G, pos)
    nx.draw_networkx_labels(G, pos, font_size=6)

    plt.title("Reduced Fat Tree Topology")

    if save_path:
        plt.savefig(save_path)

    plt.show()

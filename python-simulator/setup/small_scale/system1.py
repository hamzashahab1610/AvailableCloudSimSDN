import sys, os
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from setup.large_scale.topology_generator import (
    create_topology_from_system,
    visualize_fat_tree,
)

from classes import VNF, Node, Switch, PhysicalLink, VirtualLink, SFC, System

nodes_df = pd.read_excel(
    "setup\small_scale\Experiments - Small Scale.xlsx",
    sheet_name="Experiment 1",
    header=1,
    nrows=12,
)
switches_df = pd.read_excel(
    "setup\small_scale\Experiments - Small Scale.xlsx",
    sheet_name="Experiment 1",
    header=16,
    nrows=14,
)
physical_links_df = pd.read_excel(
    "setup\small_scale\Experiments - Small Scale.xlsx",
    sheet_name="Experiment 1",
    header=33,
    nrows=36,
)
sfc1_vnf_df = pd.read_excel(
    "setup\small_scale\Experiments - Small Scale.xlsx",
    sheet_name="Experiment 1",
    header=72,
    nrows=4,
)
sfc1_vlink_df = pd.read_excel(
    "setup\small_scale\Experiments - Small Scale.xlsx",
    sheet_name="Experiment 1",
    header=79,
    nrows=3,
)

nodes_df.dropna(inplace=True, axis=1)
switches_df.dropna(inplace=True, axis=1)
physical_links_df.dropna(inplace=True, axis=1)
sfc1_vnf_df.dropna(inplace=True, axis=1)
sfc1_vlink_df.dropna(inplace=True, axis=1)

# print(nodes_df)
# print(switches_df)
# print(physical_links_df)
# print(sfc1_vnf_df)
# print(sfc1_vlink_df)

nodes = []
switches = []
physical_links = []
sfc1_vnfs = []
sfc1_vlinks = []

for index, row in nodes_df.iterrows():
    node = Node.Node(
        name=row["Server ID"],
        cpu=row["CPU Capacity (cores)"],
        ram=row["RAM Capacity (GB)"],
        storage=row["Storage Capacity (GB)"],
        p_min=row["Min Power (W)"],
        p_max=row["Max Power (W)"],
        availability=row["MTBF (hrs)"] / (row["MTBF (hrs)"] + row["MTTR (hrs)"]),
        co2=row["CO2 (kg CO2/kWh)"],
        processing_delay=row["Processing Delay (ms)"],
        platform="1",
    )
    nodes.append(node)

for index, row in switches_df.iterrows():
    switch = Switch.Switch(
        name=row["Switch ID"],
        type="Switch",
        bandwidth=row["Bandwidth Capacity (Gbps)"],
        p_static=row["Static Power (W)"],
        p_port=row["Power per port (W)"],
        availability=row["MTBF (hrs)"] / (row["MTBF (hrs)"] + row["MTTR (hrs)"]),
        co2=row["CO2 (kg CO2/kWh)"],
        processing_delay=row["Processing Delay (ms)"],
    )
    switches.append(switch)

for index, row in physical_links_df.iterrows():
    source_node = next(
        (node for node in nodes + switches if node.name == row["Source Node"]), None
    )
    target_node = next(
        (node for node in nodes + switches if node.name == row["Target Node"]), None
    )

    if source_node and target_node:
        physical_link = PhysicalLink.PhysicalLink(
            name=row["Link ID"],
            source=source_node,
            target=target_node,
        )
        physical_links.append(physical_link)
    else:
        print(f"Warning: Could not find nodes for link {row['Link ID']}")

for index, row in sfc1_vnf_df.iterrows():
    vnf = VNF.VNF(
        name=row["VNF ID"],
        cpu=row["CPU Demand (cores)"],
        ram=row["RAM Demand (GB)"],
        storage=row["Storage Demand (GB)"],
        availability=row["MTBF (hrs)"] / (row["MTBF (hrs)"] + row["MTTR (hrs)"]),
        processing_delay=row["Processing Delay (ms)"],
        backup_of=row["Backup Of"],
        type="VNF",
    )
    sfc1_vnfs.append(vnf)

for index, row in sfc1_vlink_df.iterrows():
    source_vnf = next((vnf for vnf in sfc1_vnfs if vnf.name == row["Source VNF"]), None)
    target_vnf = next((vnf for vnf in sfc1_vnfs if vnf.name == row["Target VNF"]), None)

    if source_vnf and target_vnf:
        vlink = VirtualLink.VirtualLink(
            name=row["Virtual Link ID"],
            source=source_vnf,
            target=target_vnf,
            bandwidth=row["Bandwidth Demand (Gbps)"],
        )
        sfc1_vlinks.append(vlink)
    else:
        print(f"Warning: Could not find VNFs for link {row['Virtual Link ID']}")

sfc1 = SFC.SFC("sfc1", sfc1_vnfs, sfc1_vlinks, 100)

G,topology = create_topology_from_system(nodes, switches, physical_links)
# visualize_fat_tree(G, save_path="system_topology.png")

system = System.System(
    name="system1",
    nodes=nodes,
    switches=switches,
    physical_links=physical_links,
    time_active=24,
    weight=0.4,
    G=G,
)

# system.sfcs = [sfc1]

# sfc1.set_system(system)

system.print_system()

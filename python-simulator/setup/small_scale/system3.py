import pandas as pd
from classes import VNF, Node, Switch, PhysicalLink, VirtualLink, SFC, System

nodes_df = pd.read_excel(
    "Experiments.xlsx", sheet_name="Experiment 3", header=1, nrows=12
)
switches_df = pd.read_excel(
    "Experiments.xlsx", sheet_name="Experiment 3", header=16, nrows=14
)
physical_links_df = pd.read_excel(
    "Experiments.xlsx", sheet_name="Experiment 3", header=33, nrows=36
)
sfc1_vnf_df = pd.read_excel(
    "Experiments.xlsx", sheet_name="Experiment 3", header=72, nrows=8
)
sfc1_vlink_df = pd.read_excel(
    "Experiments.xlsx", sheet_name="Experiment 3", header=83, nrows=8
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
        row["Server ID"],
        row["CPU Capacity (cores)"],
        row["RAM Capacity (GB)"],
        row["Storage Capacity (GB)"],
        row["Min Power (W)"],
        row["Max Power (W)"],
        row["MTBF (hrs)"],
        row["MTTR (hrs)"],
        row["CO2 (kg CO2/kWh)"],
        "Node",
        row["Processing Delay (ms)"],
    )
    nodes.append(node)

for index, row in switches_df.iterrows():
    switch = Switch.Switch(
        row["Switch ID"],
        "Switch",
        row["Bandwidth Capacity (Gbps)"],
        row["Static Power (W)"],
        row["Power per port (W)"],
        row["MTBF (hrs)"],
        row["MTTR (hrs)"],
        row["CO2 (kg CO2/kWh)"],
        row["Processing Delay (ms)"],
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
            row["Link ID"],
            source_node,
            target_node,
            row["Bandwidth Capacity (Gbps)"],
            row["MTBF (hrs)"],
            row["MTTR (hrs)"],
            row["Propagation Delay (ms)"],
        )
        physical_links.append(physical_link)
    else:
        print(f"Warning: Could not find nodes for link {row['Link ID']}")

for index, row in sfc1_vnf_df.iterrows():
    vnf = VNF.VNF(
        row["VNF ID"],
        row["CPU Demand (cores)"],
        row["RAM Demand (GB)"],
        row["Storage Demand (GB)"],
        row["MTBF (hrs)"],
        row["MTTR (hrs)"],
        row["Processing Delay (ms)"],
        row["Backup Of"],
    )
    sfc1_vnfs.append(vnf)

for index, row in sfc1_vlink_df.iterrows():
    source_vnf = next((vnf for vnf in sfc1_vnfs if vnf.name == row["Source VNF"]), None)
    target_vnf = next((vnf for vnf in sfc1_vnfs if vnf.name == row["Target VNF"]), None)

    if source_vnf and target_vnf:
        vlink = VirtualLink.VirtualLink(
            row["Virtual Link ID"],
            source_vnf,
            target_vnf,
            row["Bandwidth Demand (Gbps)"],
        )
        sfc1_vlinks.append(vlink)
    else:
        print(f"Warning: Could not find VNFs for link {row['Virtual Link ID']}")

sfc1 = SFC.SFC("sfc1", sfc1_vnfs, sfc1_vlinks, 10000)

system = System.System("system3", [sfc1], nodes, switches, physical_links, 24, 0.5)

sfc1.set_system(system)

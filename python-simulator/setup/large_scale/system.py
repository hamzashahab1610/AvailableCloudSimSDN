import sys, os, json, pickle

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from classes.System import System
from setup.large_scale.topology_generator import (
    generate_fat_tree,
    convert_topology_to_json,
)


if os.path.exists("system.pkl"):
    with open("system.pkl", "rb") as file:
        system = pickle.load(file)
    print("Loaded saved system object.")

# Extract the topology from the system object
# nodes = system.nodes
# edge_switches = [
#     switch for switch in system.switches if switch.name.startswith("EdgeSwitch_")
# ]
# aggregate_switches = [
#     switch for switch in system.switches if switch.name.startswith("AggrSwitch_")
# ]
# core_switches = [
#     switch for switch in system.switches if switch.name.startswith("CoreSwitch_")
# ]
# physical_links = system.physical_links


k = 72
G, nodes, edge_switches, aggregate_switches, core_switches, physical_links = (
    generate_fat_tree(k)
)

topology_json = convert_topology_to_json(
    nodes, edge_switches, aggregate_switches, core_switches, physical_links
)

# Write to file
with open("physical.json", "w") as f:
    json.dump(topology_json, f, indent=2)

system = System(
    name="FatTreeSystem",
    nodes=nodes,
    switches=edge_switches + aggregate_switches + core_switches,
    physical_links=physical_links,
    time_active=24,
    weight=0.4,
    G=G,
)

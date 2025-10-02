import sys, os, json

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from classes import VNF, VirtualLink, SFC


def generate_sfc(sfc_id, num_vnfs):
    vnfs = []
    for i in range(num_vnfs):
        vnf = VNF.VNF(
            name=f"VNF_{sfc_id}_{i}",
            cpu=2,  # Service Function Chain Placement for Joint Cost and Latency Optimization
            ram=1,  # Service Function Chain Placement for Joint Cost and Latency Optimization
            storage=2,  # Service Function Chain Placement for Joint Cost and Latency Optimization
            availability=0.9999,  # Availability‑aware and energy‑aware dynamic SFC placement using reinforcement learning
            processing_delay=1,
            backup_of=0,
            type="vm"
        )
        vnfs.append(vnf)

    virtual_links = []
    for i in range(len(vnfs) - 1):
        vl = VirtualLink.VirtualLink(
            name=f"VL_{sfc_id}_{i}",
            source=vnfs[i],
            target=vnfs[i + 1],
            bandwidth=0.01,
        )
        virtual_links.append(vl)

    sfc = SFC.SFC(
        name=f"SFC_{sfc_id}",
        vnfs=vnfs,
        virtual_links=virtual_links,
        max_latency=100,
    )

    return sfc

def convert_to_virtual_topology(sfcs):
    topology = {
        "nodes": [],
        "policies": [],
        "links": [],
    }

    for sfc in sfcs:
        for vnf in sfc.vnfs:
            topology["nodes"].append(
                {
                    "name": vnf.name,
                    "mipoper": 800,
                    "bw": 15000,
                    "mips": 10000,
                    "type": vnf.type,
                    "pes": vnf.cpu,
                    "ram": vnf.ram,
                    "size": vnf.storage,
                    "datacenter": "dc1",
                    "host": vnf.node.name if vnf.node else None,
                    "availability": vnf.availability,
                    "processing_delay": vnf.processing_delay,
                    "backup_of": vnf.backup_of,
                }
            )

        for vlink in sfc.virtual_links:
            topology["links"].append(
              {
                "name": vlink.source.name + vlink.target.name,
                "source": vlink.source.name,
                "destination": vlink.target.name,
                "bandwidth": int(vlink.bandwidth * 1000000),
              }
            )

    return topology

def convert_to_virtual_topology_json(sfcs, topology_file):
    with open(topology_file) as f:
        cloudsim_topology = json.load(f)

    topology = {
        "nodes": cloudsim_topology["nodes"],
        "policies": cloudsim_topology["policies"],
        "links": cloudsim_topology["links"],
    }

    primary_vnfs = []

    for node in topology["nodes"]:
        vnf = next(
            (vnf for sfc in sfcs for vnf in sfc.vnfs if vnf.name == node["name"]), None
        )

        if vnf:
            primary_vnfs.append(vnf)
            node["datacenter"] = "dc1"
            node["host"] = str(vnf.node.name) if vnf.node else None
            node["availability"] = vnf.availability
            node["processing_delay"] = vnf.processing_delay
            node["backup_of"] = vnf.backup_of

    # Backup nodes are not included in the original topology file so we need to add them but multiple SFCs can share the same primary VNFs so we need to make sure that we don't add duplicate backup nodes
    backup_vnfs = []

    for sfc in sfcs:
        for vnf in sfc.vnfs:
            if vnf.backup_of != 0:
                if vnf.name not in [vnf.name for vnf in backup_vnfs]:
                    backup_vnfs.append(vnf)

    for vnf in backup_vnfs:
        topology["nodes"].append(
            {
                "name": str(vnf.name),
                "mipoper": 800,
                "bw": 15000,
                "mips": 1000,
                "type": next(
                    (
                        primary_vnf.type
                        for primary_vnf in primary_vnfs
                        if primary_vnf.name == vnf.backup_of
                    ),
                    None,
                ),
                "pes": vnf.cpu,
                "ram": vnf.ram,
                "size": vnf.storage,
                "datacenter": "dc1",
                "host": str(vnf.node.name) if vnf.node else None,
                "availability": vnf.availability,
                "processing_delay": vnf.processing_delay,
                "backup_of": vnf.backup_of,
            }
        )

    return topology


def build_sfcs_from_topology(topology_file):
    with open(topology_file) as f:
        topology = json.load(f)

    vnfs = []
    for node in topology["nodes"]:
        vnf = VNF.VNF(
            name=node["name"],
            type=node["type"],
            cpu=node["pes"],
            ram=node["ram"],
            storage=node["size"],
            availability=0.9999,
            processing_delay=1,
            backup_of=0,
        )
        vnfs.append(vnf)

    sfcs = []
    for policy in topology["policies"]:
        complete_chain = [policy["source"]] + policy["sfc"] + [policy["destination"]]
        chain_vnfs = []

        for name in complete_chain:
            matching_vnf = next(vnf for vnf in vnfs if vnf.name == name)
            chain_vnfs.append(matching_vnf)

        vlinks = []
        for i in range(len(chain_vnfs) - 1):
            bandwidth = 2000000 / 1000000
            for link in topology["links"]:
                if (
                    link["source"] == chain_vnfs[i].name
                    and link["destination"] == chain_vnfs[i + 1].name
                ):
                    bandwidth = link.get("bandwidth", 2000000) / 1000000
                    break

            vlink = VirtualLink.VirtualLink(
                name=f"{chain_vnfs[i].name}_{chain_vnfs[i+1].name}",
                source=chain_vnfs[i],
                target=chain_vnfs[i + 1],
                bandwidth=bandwidth,
            )
            vlinks.append(vlink)

        sfc = SFC.SFC(
            name=policy["name"],
            vnfs=chain_vnfs,
            virtual_links=vlinks,
            max_latency=policy["expected_time"],
        )
        sfcs.append(sfc)

    return sfcs


# Generate multiple SFCs
# num_sfcs = 1  # Number of SFCs to generate
# sfcs = []

# for sfc_id in range(num_sfcs):
#     num_vnfs = 3
#     sfc = generate_sfc(sfc_id, num_vnfs)
#     sfcs.append(sfc)

topology_file = "virtual-serial.json"
# topology_file = "virtual-split-join.json"

sfcs = build_sfcs_from_topology(topology_file)
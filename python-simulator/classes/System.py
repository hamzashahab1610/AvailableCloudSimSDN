from classes import PathFinder


class System:
    def __init__(self, name, nodes, switches, physical_links, time_active, weight, G):
        self.name = name
        self.sfcs = []
        self.nodes = nodes
        self.switches = switches
        self.physical_links = physical_links
        self.redundancy = 0
        self.latency = 0
        self.availability = 1
        self.carbon_footprint = 0
        self.graph = G
        self.path_finder = PathFinder.PathFinder(G, switches)
        self.paths = []
        self.weight = weight
        self.time_active = time_active
        self.solution = []
        self.objective = 0
        self.min_availability = 0.9
        self.min_carbon_footprint = 10000
        self.max_carbon_footprint = 100000
        self.candidate_nodes = []

    def reset(self):
        self.solution = []
        self.objective = 0
        self.redundancy = 0
        self.latency = 0
        self.availability = 1
        self.carbon_footprint = 0
        self.candidate_nodes = []

        for node in self.nodes:
            node.cpu = node.total_cpu
            node.ram = node.total_ram
            node.storage = node.total_storage
            node.availability = node.initial_availability
            node.vnfs = []
            node.cpu_utilization = 0
            node.ram_utilization = 0
            node.storage_utilization = 0
            node.carbon_footprint = node.calculate_min_carbon_footprint(
                self.time_active
            )

        for switch in self.switches:
            switch.bandwidth = switch.total_bandwidth
            switch.virtual_links = []
            switch.active_ports = 0
            switch.bandwidth_utilization = 0
            switch.carbon_footprint = 0

        for sfc in self.sfcs:
            sfc.latency = 0
            sfc.availability = 1

            sfc.vnfs = [vnf for vnf in sfc.vnfs if vnf.backup_of == 0]

            for vnf in sfc.vnfs:
                vnf.node = None

            for vl in sfc.virtual_links:
                vl.path = None

    def get_candidate_path(self, source, target):
        return self.path_finder.get_path(source, target)

    def check_node_resource_constraints(self, node, vnf):
        if node.cpu < vnf.cpu:
            return False

        if node.ram < vnf.ram:
            return False

        if node.storage < vnf.storage:
            return False

        return True

    def vnf_placement(self, vnf, node, sfc):
        if not self.check_node_resource_constraints(node, vnf):
            return False

        node.add_vnf(vnf)
        vnf.node = node

        sfc.latency += vnf.processing_delay + vnf.node.processing_delay

        return True

    def place_redundant_vnf(
        self, vnf, redundant_instances, candidate_nodes, sfc, backup_num=None
    ):
        self.candidate_nodes = candidate_nodes.copy()

        # Keep track of nodes that already have this VNF or its backups
        used_nodes = []
        
        # Add the primary VNF's node to the used_nodes list
        if vnf.node:
            used_nodes.append(vnf.node)

        # Add nodes of existing backup VNFs to the used_nodes list
        primary_name = vnf.name.split("-backup-")[0]

        for existing_vnf in sfc.vnfs:
            if (existing_vnf.name == primary_name or 
                existing_vnf.name.startswith(primary_name + "-backup-")) and existing_vnf.node:
                if existing_vnf.node not in used_nodes:
                    used_nodes.append(existing_vnf.node)

        for i in range(redundant_instances):
            current_backup_num = backup_num
            
            if current_backup_num is None:
              current_backup_num = len(
                  [
                      v
                      for v in sfc.vnfs
                      if v.name.startswith(vnf.name.split("-backup-")[0] + "-backup-")
                  ]
              )

            backup_vnf = sfc.create_backup_vnf(
                vnf,
                current_backup_num,
            )
            placed = False

            while self.candidate_nodes and not placed:
                node = self.candidate_nodes[0]
                success = self.vnf_placement(backup_vnf, node, sfc)

                if success:
                    placed = True
                else:
                    self.candidate_nodes.pop(0)

            if not placed:
                return False

        return True, self.candidate_nodes

    def check_switch_resource_constraints(self, switch, virtual_link):
        if switch.bandwidth < virtual_link.bandwidth:
            print(f"Switch {switch.name} does not have enough bandwidth.")
            print(
                f"Switch Bandwidth: {switch.bandwidth}, Virtual Link Bandwidth: {virtual_link.bandwidth}"
            )
            return False

        return True

    def virtual_link_mapping(self, virtual_link, path, sfc):
        for switch in path.primary_channel.switches:
            if not self.check_switch_resource_constraints(switch, virtual_link):
                return False

            switch.add_virtual_link(virtual_link)
            switch.active_ports += 1

        virtual_link.path = path
        sfc.latency += path.path_latency

        return True

    def calculate_availability(self):
        self.availability = sum(
            [sfc.calculate_availability() for sfc in self.sfcs]
        ) / len(self.sfcs)

        return self.availability

    def calculate_latency(self):
        self.latency = sum([sfc.latency for sfc in self.sfcs])

        return self.latency

    def calculate_redundancy(self):
        self.redundancy = sum(len(sfc.vnfs) for sfc in self.sfcs)

        return self.redundancy

    def get_unique_nodes_and_switches(self):
        unique_nodes = []
        unique_switches = []

        for sfc in self.sfcs:
            for vnf in sfc.vnfs:
                if vnf.node and vnf.node not in unique_nodes:
                    unique_nodes.append(vnf.node)

            for virtual_link in sfc.virtual_links:
                if virtual_link.path and virtual_link.path.primary_channel:
                    for switch in virtual_link.path.primary_channel.switches:
                        if switch and switch not in unique_switches:
                            unique_switches.append(switch)

        return unique_nodes, unique_switches

    def calculate_carbon_footprint(self):
        self.carbon_footprint = 0
        unique_nodes, unique_switches = self.get_unique_nodes_and_switches()

        for node in unique_nodes:
            self.carbon_footprint += node.calculate_carbon_footprint(self.time_active)

        for switch in unique_switches:
            self.carbon_footprint += switch.calculate_carbon_footprint(self.time_active)

        return self.carbon_footprint

    def calculate_objective(self):
        self.calculate_latency()
        self.calculate_redundancy()
        self.calculate_availability()
        self.calculate_carbon_footprint()

        normalized_availability = (self.availability - self.min_availability) / (
            1 - self.min_availability
        )
        normalized_carbon_footprint = (
            self.carbon_footprint - self.min_carbon_footprint
        ) / (self.max_carbon_footprint - self.min_carbon_footprint)

        self.objective = (self.weight * normalized_availability) - (
            ((1 - self.weight) * normalized_carbon_footprint)
        )

        return self.objective

    def print_system(self):
        print()
        print(f"System: {self.name}")
        print("================")
        print(f"SFCs: {len(self.sfcs)}")
        print(f"Nodes: {len(self.nodes)}")
        print(f"Switches: {len(self.switches)}")
        print(f"Physical Links: {len(self.physical_links)}")
        print()

        print("Nodes Status:")
        print("------------")
        for node in self.nodes:
            print(
                f"Node: {node.name}"
                f" -> Availability: {round((node.availability*100),2)} %"
                f", Delay: {node.processing_delay} ms"
                f", Carbon Footprint: {round(node.carbon_footprint,2)} kgCO2"
            )
        print()

        # print("Switches Status:")
        # print("--------------")
        # for switch in self.switches:
        #     print(
        #         f"Switch: {switch.name}"
        #         f" -> Availability: {round((switch.availability*100),2)} %"
        #         f", Delay: {switch.processing_delay} ms"
        #         f", Carbon Footprint: {round(switch.carbon_footprint,2)} kgCO2"
        #     )
        # print()

        print("VNFs Status:")
        print("------------")
        for sfc in self.sfcs:
            print(f"SFC: {sfc.name}")
            print("================")
            print(f"VNFs: {len(sfc.vnfs)}")
            print(f"Virtual Links: {len(sfc.virtual_links)}")
            print("------------")
            for vnf in sfc.vnfs:
                print(
                    f"VNF: {vnf.name}"
                    f" -> Availability: {round((vnf.availability*100),2)} %"
                    f", Delay: {vnf.processing_delay} ms"
                )

            print()

            for virtual_link in sfc.virtual_links:
                print(
                    f"Virtual Link: {virtual_link.name}"
                    f" -> Bandwidth: {virtual_link.bandwidth} Gbps"
                )

    def print_placement(self):
        self.calculate_objective()
        unique_nodes, unique_switches = self.get_unique_nodes_and_switches()

        print()
        print(f"System: {self.name}")
        print("================")

        for sfc in self.sfcs:
            print(f"SFC: {sfc.name}")
            print("================")
            print(f"VNFs: {len(sfc.vnfs)}")
            print(f"Virtual Links: {len(sfc.virtual_links)}")
            print(f"Avaialbility: {sfc.availability}")
            print(f"Latency: {sfc.latency}")
            print("------------")
            print("VNF Mapping:")
            print("------------")
            for vnf in sfc.vnfs:
                print(
                    f"VNF: {vnf.name} -> Node: {vnf.node.name}, Availability: {round((vnf.node.availability*100),2)} %, Delay: {vnf.processing_delay + vnf.node.processing_delay} ms"
                )

            print()

            print("Link Mapping:")
            print("-------------")
            print_str = ""
            for virtual_link in sfc.virtual_links:
                print_str += f"Virtual Link: {virtual_link.name} -> Path: {virtual_link.path.name}, Availability: {round((virtual_link.path.availability*100),5)} %, Delay: {round(virtual_link.path.latency,2)} ms, Carbon Footprint: {round(virtual_link.path.carbon_footprint,2)} kgCO2\n"

            print(print_str)

        #     for channel in virtual_link.path.channels:
        #         print_str = f"Virtual Link: {virtual_link.name} -> Channel: {channel.name}, Availability: {round((channel.availability*100),5)} %, Delay: {round(channel.latency,2)} ms"

        #         print(print_str)

        # print()

        # print("Nodes Status:")
        # print("------------")
        # for node in unique_nodes:
        #     print(
        #         f"Node: {node.name}"
        #         f" -> CPU: {round((node.cpu_utilization*100),2)} %"
        #         f", RAM: {round((node.ram_utilization)*100,2)} %"
        #         f", Storage: {round((node.storage_utilization)*100,2)} %"
        #         f", Availability: {round((node.initial_availability)*100,2)} %"
        #         f", Carbon Footprint: {round(node.carbon_footprint,2)} kgCO2"
        #         f", Platform: {node.platform}"
        #         # f", VNFs: {', '.join([vnf.name for vnf in node.vnfs])}"
        #     )
        # print()

        # print("Switches Status:")
        # print("--------------")
        # for switch in unique_switches:
        #     if switch.name != "internal":
        #         print(
        #             f"Switch: {switch.name}"
        #             f" -> Bandwidth: {round(switch.bandwidth_utilization*100,2)} %"
        #             f", Active Ports: {switch.active_ports}"
        #             f", Availability: {round((switch.availability*100),2)} %"
        #             f", Carbon Footprint: {round(switch.carbon_footprint,2)} kgCO2"
        #         )
        # print()

        print(f"Availability: {self.availability}")
        print(f"Carbon Footprint: {self.carbon_footprint}")
        print(f"Latency: {self.latency}")
        print(f"Redundancy: {self.redundancy}")
        print(f"Solution: {list(self.solution)}")
        print(f"Objective: {self.objective}")
        # print()

    def __str__(self):
        return f"System: {self.name}, SFCs: {self.sfcs}, Nodes: {self.nodes}, Physical Links: {self.physical_links}, Availability: {self.availability}, Carbon Footprint: {self.carbon_footprint}"

    def __repr__(self):
        return str(self)

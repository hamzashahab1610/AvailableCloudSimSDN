class SFC:
    def __init__(self, name, vnfs, virtual_links, max_latency):
        self.name = name
        self.vnfs = vnfs
        self.virtual_links = virtual_links
        self.latency = 0
        self.availability = 1
        self.max_latency = max_latency
        self.system = None

    def set_system(self, system):
        self.system = system

    def create_backup_vnf(self, vnf, i):
        # Extract base name if this is already a backup VNF
        base_name = vnf.name
        if "-backup-" in base_name:
            base_name = base_name.split("-backup-")[0]

        backup_vnf = vnf.clone()

        # Add the backup index here where we have context of existing backups
        backup_vnf.name = f"{base_name}-backup-{i}"
        backup_vnf.backup_of = vnf.name

        self.vnfs.append(backup_vnf)

        return self.vnfs[-1]

    def calculate_availability(self):
        self.availability = 1.0
        processed_vnfs = set()

        for vnf in self.vnfs:
            if vnf.name not in processed_vnfs:
                vnf_availability = 1.0

                for vnf_instance in self.get_redundant_vnfs(vnf):
                    vnf_availability *= 1 - (
                        vnf_instance.availability
                        * vnf_instance.node.initial_availability
                    )

                    processed_vnfs.add(vnf_instance.name)

                self.availability *= 1 - vnf_availability

        processed_links = set()

        for vl in self.virtual_links:
            source_vnf = vl.source
            target_vnf = vl.target
            vnf_pair = (source_vnf.name, target_vnf.name)

            if vnf_pair in processed_links:
                continue

            source_vnfs = self.get_redundant_vnfs(source_vnf)
            target_vnfs = self.get_redundant_vnfs(target_vnf)

            link_unavailability = 1.0

            for src_vnf in source_vnfs:
                for tgt_vnf in target_vnfs:
                    path = self.system.get_candidate_path(src_vnf.node, tgt_vnf.node)

                    if path:
                        link_unavailability *= 1 - path.path_availability

            link_availability = 1 - link_unavailability
            self.availability *= link_availability
            processed_links.add(vnf_pair)

        return self.availability

    def get_redundant_vnfs(self, vnf):
        redundant_vnfs = [
            v for v in self.vnfs if v.name == vnf.name or v.backup_of == vnf.name
        ]
        return redundant_vnfs

    def __str__(self):
        return f"SFC: {self.name}, VNFs: {self.vnfs}, Virtual Links: {self.virtual_links}, Latency: {self.latency}, Availability: {self.availability}"

    def __repr__(self):
        return str(self)

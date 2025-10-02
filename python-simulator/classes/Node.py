class Node:
    def __init__(
        self,
        name,
        platform,
        cpu,
        ram,
        storage,
        mtbf,
        mttr,
        availability,
        p_min,
        p_max,
        co2,
        processing_delay,
        time_active=24,
        type="Node",
        datacenter=None,
    ):
        self.name = name
        self.platform = platform
        self.type = type
        self.cpu = cpu
        self.ram = ram
        self.storage = storage
        self.total_cpu = cpu
        self.total_ram = ram
        self.total_storage = storage
        self.mtbf = mtbf
        self.mttr = mttr
        self.availability = availability
        self.initial_availability = availability
        self.p_min = p_min
        self.p_max = p_max
        self.co2 = co2
        self.vnfs = []
        self.time_active = time_active
        self.cpu_utilization = self.calculate_cpu_utilization()
        self.ram_utilization = self.calculate_ram_utilization()
        self.storage_utilization = self.calculate_storage_utilization()
        self.carbon_footprint = self.calculate_carbon_footprint(time_active)
        self.min_carbon_footprint = self.calculate_min_carbon_footprint(time_active)
        self.max_carbon_footprint = self.calculate_max_carbon_footprint(time_active)
        self.processing_delay = processing_delay
        self.datacenter = datacenter

    def add_vnf(self, vnf):
        self.vnfs.append(vnf)
        self.cpu -= vnf.cpu
        self.ram -= vnf.ram
        self.storage -= vnf.storage
        self.calculate_cpu_utilization()
        self.calculate_ram_utilization()
        self.calculate_storage_utilization()
        self.calculate_availability()
        self.calculate_carbon_footprint(self.time_active)

    def calculate_cpu_utilization(self):
        self.cpu_utilization = (self.total_cpu - self.cpu) / self.total_cpu
        return self.cpu_utilization

    def calculate_ram_utilization(self):
        self.ram_utilization = (self.total_ram - self.ram) / self.total_ram
        return self.ram_utilization

    def calculate_storage_utilization(self):
        self.storage_utilization = (
            self.total_storage - self.storage
        ) / self.total_storage

        return self.storage_utilization

    def calculate_availability(self):
        self.availability = self.availability

        if len(self.vnfs) > 0:
            for vnf in self.vnfs:
                self.availability *= vnf.availability

        return self.availability

    def calculate_carbon_footprint(self, time):
        self.carbon_footprint = (
            (self.p_min + (self.p_max - self.p_min) * self.cpu_utilization) * time
        ) * self.co2
        return self.carbon_footprint

    def calculate_min_carbon_footprint(self, time):
        self.min_carbon_footprint = self.p_min * time * self.co2
        return self.min_carbon_footprint

    def calculate_max_carbon_footprint(self, time):
        self.max_carbon_footprint = self.p_max * time * self.co2
        return self.max_carbon_footprint

    def __str__(self):
        return f"Node: {self.name}, CPU: {self.cpu}, RAM: {self.ram}, Storage: {self.storage}, Total CPU: {self.total_cpu}, Total RAM: {self.total_ram}, Total Storage: {self.total_storage}, Pmin: {self.p_min}, Pmax: {self.p_max}, Processing Delay: {self.processing_delay}, CO2: {self.co2}, VNFS: {self.vnfs}, CPU Utilization: {self.cpu_utilization}, Carbon Footprint: {self.carbon_footprint}, Availability: {self.availability}"

    def __repr__(self):
        return str(self)

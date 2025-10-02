class Switch:
    def __init__(
        self,
        name,
        type,
        bandwidth,
        availability,
        p_static,
        p_port,
        co2,
        processing_delay,
        datacenter=None,
    ):
        self.name = name
        self.type = type
        self.bandwidth = bandwidth
        self.availability = availability
        self.p_static = p_static
        self.p_port = p_port
        self.co2 = co2
        self.processing_delay = processing_delay
        self.total_bandwidth = bandwidth
        self.virtual_links = []
        self.carbon_footprint = 0
        self.active_ports = 0
        self.bandwidth_utilization = self.calculate_bandwidth_utilization()
        self.datacenter = datacenter

    def add_virtual_link(self, virtual_link):
        self.virtual_links.append(virtual_link)
        self.bandwidth -= virtual_link.bandwidth
        self.calculate_bandwidth_utilization()

    def calculate_bandwidth_utilization(self):
        self.bandwidth_utilization = (
            self.total_bandwidth - self.bandwidth
        ) / self.total_bandwidth

        return self.bandwidth_utilization

    def calculate_active_ports(self):
        self.active_ports = 0

        self.active_ports = len(self.virtual_links)

        return self.active_ports

    def calculate_carbon_footprint(self, time):
        self.calculate_active_ports()
        self.carbon_footprint = (
            (self.p_static + self.p_port * self.active_ports) * time
        ) * self.co2
        return self.carbon_footprint

    def __str__(self):
        return f"Switch: {self.name}, Bandwidth: {self.bandwidth}, Processing Delay: {self.processing_delay}, Total Bandwidth: {self.total_bandwidth}, Carbon Footprint: {self.carbon_footprint}, Availability: {self.availability}"

    def __repr__(self):
        return str(self)

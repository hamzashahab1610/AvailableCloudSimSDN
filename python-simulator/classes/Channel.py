class Channel:
    def __init__(self, name, source, target, switches, time_active):
        self.name = name
        self.source = source
        self.target = target
        self.switches = switches
        self.availability = self.calculate_availability()
        self.latency = self.calculate_latency()
        self.channel_latency = self.calculate_channel_latency()
        self.time_active = time_active
        self.carbon_footprint = self.calculate_carbon_footprint()

    def calculate_availability(self):
        self.availability = 1
        for switch in self.switches:
            self.availability *= switch.availability

        return self.availability

    def calculate_latency(self):
        self.latency = 0

        if self.source == self.target:
            self.latency = self.source.processing_delay
            return self.latency
        else:
            self.latency += self.source.processing_delay
            self.latency += self.target.processing_delay

        for switch in self.switches:
            self.latency += switch.processing_delay

        return self.latency

    def calculate_channel_latency(self):
        self.channel_latency = 0

        for switch in self.switches:
            self.channel_latency += switch.processing_delay

        return self.channel_latency

    def calculate_carbon_footprint(self):
        self.carbon_footprint = 0

        if self.source == self.target:
            self.carbon_footprint = self.source.calculate_carbon_footprint(
                self.time_active
            )
            return self.carbon_footprint
        else:
            self.carbon_footprint += self.source.calculate_carbon_footprint(
                self.time_active
            )
            self.carbon_footprint += self.target.calculate_carbon_footprint(
                self.time_active
            )

        for switch in self.switches:
            self.carbon_footprint += switch.calculate_carbon_footprint(self.time_active)

        return self.carbon_footprint

    def __str__(self):
        return f"Channel {self.name} from {self.source.name} to {self.target.name} with availability {self.availability}, latency {self.latency}, and carbon footprint {self.carbon_footprint}"

    def __repr__(self):
        return str(self)

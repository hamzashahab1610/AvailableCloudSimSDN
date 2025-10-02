class Path:
    def __init__(self, name, source, target, channels):
        self.name = name
        self.source = source
        self.target = target
        self.channels = channels
        self.primary_channel = min(self.channels, key=lambda x: x.latency)
        self.common_switches, self.unique_switches = (
            self.get_common_and_unique_switches()
        )
        self.availability = self.calculate_availability()
        self.path_availability = self.calculate_path_availability()
        self.latency = self.primary_channel.latency
        self.path_latency = self.primary_channel.channel_latency
        self.carbon_footprint = self.calculate_carbon_footprint()

    def get_common_and_unique_switches(self):
        channel_switch_sets = [set(channel.switches) for channel in self.channels]

        if channel_switch_sets:
            common_switches = set.intersection(*channel_switch_sets)
        else:
            common_switches = set()

        return common_switches, set.union(*channel_switch_sets) - common_switches

    def calculate_availability(self):
        self.availability = self.source.availability * self.target.availability

        # if len(self.common_switches) > 0:
        #     for switch in self.common_switches:
        #         self.availability *= switch.availability

        # if len(self.unique_switches) > 0:
        #     unique_switch_availability = 1
        #     for switch in self.unique_switches:
        #         unique_switch_availability *= 1 - switch.availability

        #     self.availability *= 1 - unique_switch_availability

        channel_availability = 1
        for channel in self.channels:
            channel_availability *= 1 - channel.availability

        self.availability *= 1 - channel_availability

        return self.availability

    def calculate_path_availability(self):
        self.path_availability = 1

        # if len(self.common_switches) > 0:
        #     for switch in self.common_switches:
        #         self.path_availability *= switch.availability

        # if len(self.unique_switches) > 0:
        #     unique_switch_availability = 1
        #     for switch in self.unique_switches:
        #         unique_switch_availability *= 1 - switch.availability

        #     self.path_availability *= 1 - unique_switch_availability

        channel_availability = 1
        for channel in self.channels:
            channel_availability *= 1 - channel.availability

        self.path_availability *= 1 - channel_availability

        return self.path_availability

    def calculate_carbon_footprint(self):
        self.carbon_footprint = 0

        self.carbon_footprint += self.source.carbon_footprint
        self.carbon_footprint += self.target.carbon_footprint

        if len(self.common_switches) > 0:
            for switch in self.common_switches:
                self.carbon_footprint += switch.carbon_footprint

        if len(self.unique_switches) > 0:
            for switch in self.unique_switches:
                self.carbon_footprint += switch.carbon_footprint

        return self.carbon_footprint

    def __str__(self):
        return f"Path: {self.name}, Source: {self.source}, Target: {self.target}, Physical Links: {self.physical_links}, Switches: {self.switches}, Availability: {self.availability}, Path Availability: {self.path_availability}, Latency: {self.latency}, Path Latency: {self.path_latency}, Carbon Footprint: {self.carbon_footprint}"

    def __repr__(self):
        return str(self)

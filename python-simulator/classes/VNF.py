class VNF:
    def __init__(
        self, name, type, cpu, ram, storage, availability, processing_delay, backup_of
    ):
        self.name = name
        self.type = type
        self.cpu = cpu
        self.ram = ram
        self.storage = storage
        self.availability = availability
        self.processing_delay = processing_delay
        self.backup_of = backup_of
        self.node = None

    def clone(self):
        return VNF(
            self.name,
            self.type,
            self.cpu,
            self.ram,
            self.storage,
            self.availability,
            self.processing_delay,
            self.backup_of,
        )

    def __str__(self):
        return f"VNF: {self.name}, Type: {self.type}, CPU: {self.cpu}, RAM: {self.ram}, Storage: {self.storage},  Processing Delay: {self.processing_delay}, Node: {self.node}, Availability: {self.availability}"

    def __repr__(self):
        return str(self)

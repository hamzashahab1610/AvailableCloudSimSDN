class VirtualLink:
    def __init__(self, name, source, target, bandwidth):
        self.name = name
        self.source = source
        self.target = target
        self.bandwidth = bandwidth
        self.path = None

    def __str__(self):
        return f"Virtual Link: {self.name}, Source: {self.source}, Target: {self.target}, Bandwidth: {self.bandwidth}, Path: {self.path}"

    def __repr__(self):
        return str(self)

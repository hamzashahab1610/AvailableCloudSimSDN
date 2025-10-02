class PhysicalLink:
    def __init__(self, name, source, target):
        self.name = name
        self.source = source
        self.target = target

    def __str__(self):
        return (
            f"Physical Link: {self.name}, Source: {self.source}, Target: {self.target}"
        )

    def __repr__(self):
        return str(self)

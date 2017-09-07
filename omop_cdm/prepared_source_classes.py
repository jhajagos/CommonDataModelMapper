from mapping_classes import InputClass


class SourcePersonObject(InputClass):
    """A person"""

    def fields(self):
        return []


class SourceObservationPeriodObject(InputClass):
    """An observation period for the person"""

    def fields(self):
        return []


class SourceEncounterObject(InputClass):
    """An encounter"""

    def fields(self):
        return []

class SourceResultObject(InputClass):
    """Result"""

    def fields(self):
        return []
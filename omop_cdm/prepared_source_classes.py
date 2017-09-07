from mapping_classes import InputClass

class PreparedSourceObject(InputClass):

    def _parent_class_fields(self):
        return ["i_exclude"]

    def _post_process_fields(self, field_list):
        if len(field_list):
            for additional_field in self._parent_class_fields():
                if additional_field not in field_list:
                    field_list += [additional_field]

        return field_list

    def fields(self):
        return self._post_process_fields(self._fields())

    def _fields(self):
        return []


class SourcePersonObject(PreparedSourceObject):
    """A person"""

    def _fields(self):
        return []


class SourceObservationPeriodObject(PreparedSourceObject):
    """An observation period for the person"""

    def _fields(self):
        return []


class SourceEncounterObject(PreparedSourceObject):
    """An encounter"""

    def _fields(self):
        return []

class SourceResultObject(PreparedSourceObject):
    """Result"""

    def _fields(self):
        return []
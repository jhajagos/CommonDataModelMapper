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
        return ["s_person_id", "s_gender", "m_gender", "s_birth_datetime", "s_death_datetime", "s_race",
                "m_race", "s_ethnicity", "m_ethnicity", "k_location"]


class SourceObservationPeriodObject(PreparedSourceObject):
    """An observation period for the person"""

    def _fields(self):
        return ["s_person_id", "s_start_observation_datetime", "s_end_observation_datetime"]


class SourceEncounterObject(PreparedSourceObject):
    """An encounter"""

    def _fields(self):
        return ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type",
                "m_visit_type", "i_exclude"]


class SourceResultObject(PreparedSourceObject):
    """Result"""

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_obtained_datetime", "s_type_name", "s_type_code", "m_type_code_oid",
                "s_result_text", "s_result_numeric", "s_result_datetime", "s_result_code", "m_result_code_oid",
                "s_result_unit", "s_result_unit_code", "m_result_unit_code_oid",
                "s_result_numeric_lower", "s_result_numeric_upper", "i_exclude"]


class SourceConditionObject(PreparedSourceObject):

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_start_condition_datetime", "s_end_condition_datetime",
                "s_condition_code", "m_condition_code_oid", "s_sequence_id", "m_rank", "s_condition_type", "s_present_on_admission_indicator"]


class SourceProcedureObject(PreparedSourceObject):

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_start_procedure_datetime", "s_end_procedure_datetime",
                "s_procedure_code", "m_procedure_code_oid", "s_sequence_id", "s_rank"]


class SourceMedicationObject(PreparedSourceObject):

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_drug_code", "m_drug_code_oid", "s_drug_text",
                "s_start_medication_datetime", "s_end_medication_datetime",
                "s_route", "s_quantity", "s_dose", "s_dose_unit", "s_status", "s_drug_type"]
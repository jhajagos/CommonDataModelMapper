from mapping_classes import InputClass


class PreparedSourceObject(InputClass):
    """Parent class"""
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
    """An encounter or visit"""

    def _fields(self):
        return ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type",
                "m_visit_type", "k_care_site", "s_discharge_to", "m_discharge_to",
                "s_admitting_source", "m_admitting_source", "i_exclude"]


class SourceResultObject(PreparedSourceObject):
    """Result: labs and procedures"""

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_obtained_datetime", "s_name", "s_code", "s_type_code", "m_type_code_oid",
                "s_result_text", "m_result_text",
                "s_result_numeric", "s_result_datetime", "s_result_code", "m_result_code_oid",
                "s_result_unit", "s_result_unit_code", "m_result_unit_code_oid",
                "s_result_numeric_lower", "s_result_numeric_upper", "i_exclude"]


class SourceConditionObject(PreparedSourceObject):
    """Conditions: Diagnosis codes"""
    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_start_condition_datetime", "s_end_condition_datetime",
                "s_condition_code", "s_condition_code_type", "m_condition_code_oid", "s_sequence_id", "s_rank",
                "m_rank", "s_condition_type", "s_present_on_admission_indicator", "i_exclude"]


class SourceProcedureObject(PreparedSourceObject):
    """Procedures"""
    def _fields(self):
        return ["s_person_id", "s_encounter_id",
                "s_start_procedure_datetime", "s_end_procedure_datetime",
                "s_procedure_code", "s_procedure_code_type", "m_procedure_code_oid",
                "s_sequence_id", "s_rank", "m_rank",
                "i_exclude"]


class SourceMedicationObject(PreparedSourceObject):
    """Ordered, administered medications and drugs"""
    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_drug_code", "s_drug_code_type",
                "m_drug_code_oid", "s_drug_text",
                "s_drug_alternative_text",
                "s_start_medication_datetime", "s_end_medication_datetime",
                "s_route", "m_route",
                "s_quantity",
                "s_dose", "m_dose",
                "s_dose_unit", "m_dose_unit",
                "s_status",
                "s_drug_type", "m_drug_type",
                "i_exclude"]


class SourceEncounterCoverageObject(PreparedSourceObject):
    """Even though encounter is not included in CDM we include it as this is how the hosptial sees it"""

    def _fields(self):
        return ["s_person_id", "s_encounter_id", "s_start_payer_date", "s_end_payer_date",
                "s_payer_name", "m_payer_name", "s_plan_name", "m_plan_name"]


class SourceCareSiteObject(PreparedSourceObject):
    def _fields(self):
        return ["k_care_site", "s_care_site_name"]


class SourceProviderObject(PreparedSourceObject):
    def _fields(self):
        return ["k_provider", "s_provider_name", "s_npi"]

from mapping_classes import InputClass


class PHDPersonObject(InputClass):
    def fields(self):
        return [
            "empi_id",
            "birth_date",
            "gender_display",
            "address_type_display",
            "address_type_raw_code",
            "address_line_1",
            "address_line_2",
            "address_line_3",
            "city",
            "state_display",
            "postal_cd",
            "county_display",
            "country_display",
            "address_source_description",
            "full_name",
            "prefix",
            "suffix",
            "given_name1",
            "given_name2",
            "given_name3",
            "family_name1"]

from mapping_classes import MapperClass

class DateSplit(MapperClass):
    """Split a date"""
    def map(self, date_dict):
        key = date_dict.keys()[0]
        date_string = date_dict[key]
        year, month, day = date_string.split("-")
        try:
            int_year = int(year)
        except ValueError:
            int_year = None

        try:
            int_month = int(month)
        except ValueError:
            int_month = None

        try:
            int_day = int(day)
        except ValueError:
            int_day = None

        return {"year": int_year, "month": int_month, "day": int_day}


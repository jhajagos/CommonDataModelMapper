## Mapping to OMOP CDM

There is two step process to transform source CSV files into the OMOP/OHDSI 
common data model compatible CSV files. The first step is mapping the source file into the 
prepared source file format. Examples of files in the prepared source format 
can be seen in the `./test/input/` directory.

If files are formatted into prepared source then the script 
`transform_prepared_source_to_cdm.py` can be run to convert the input files 
into output files.

## Prepared source specification

The prepared source is an intermediary format for mapping to OHDSI format.

Prefix nomenclature:

* "s_" source values
* "m_" mapped values
* "k_" and key value which maps to another value in a table

Date formats:

* Date -- "2017-01-01"
* Date with local time -- "2012-02-03 05:34"

### source_person

* s_person_id -- Source identifier for patient or person
* s_gender	-- Source gender for person
* m_gender	{MALE, FEMALE, UNKNOWN}
* s_birth_datetime -- Date of birth can be either a date or a date time
* s_death_datetime -- Date of death can either be either a date or a date time
* s_race -- Source race value for person
* m_race -- {White, Black or African American, American Indian or Alaska native, . .}
* s_ethnicity -- Source ethnicity for person
* m_ethnicity -- Mapped value {Not Hispanic or Latino, Hispanic or Latino}
* k_location -- 

### source_care_site

* k_care_site -- 
* s_care_site_name -- 

### source_encounter

* s_encounter_id -- Source identifier for encounter
* s_person_id -- Source identifier for patient or person
* s_visit_start_datetime -- Start date or date time or admission date or time 
* s_visit_end_datetime -- End date or date time or discharge date or time
* s_visit_type -- 
* m_visit_type -- 
* k_care_site -- 
* s_discharge_to -- 
* m_discharge_to -- 
* s_admitting_source -- 
* m_admitting_source -- 


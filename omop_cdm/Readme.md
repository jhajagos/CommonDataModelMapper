## Mapping to OMOP CDM

There is two step process to transform source CSV files into the OMOP/OHDSI 
common data model compatible CSV files. The first step is mapping the source file into the 
prepared source file format. Examples of files in the prepared source format 
can be seen in the `./test/input/` directory.

If files are formatted into prepared source then the script 
`transform_prepared_source_to_cdm.py` can be run to convert the input files 
into output files.

The scripts in the project require Python 3.6.

## Create a JSON config file

The first step is to create a JSON file which configures the directory location.
```json
{
  "json_map_directory":   "/external/ohdsi/vocab_download_v5_{5C1C4C11-E4B6-6175-6463-74B6F51BCA07}/",
  "csv_input_directory":  "/external/cdm_data/input/",
  "csv_output_directory": "/external/cdm_data/output/",
  "connection_uri": "postgresql+psycopg2://username:password@localhost/ohdsi",
  "schema": "mapped_data_cdm"
}
```

## Generating vocabulary JSON lookup

Before running the `transform_prepared_source_to_cdm.py` vocabulary needs to generate JSON 
look up files. The vocabulary files need to be downloaded from: http://www.ohdsi.org/web/athena/

## Mapping from source vocabulary to prepared_source

## Mapping prepared_source to OHDSI

## Create database schema

```bash
psql ohdsi < echo "create schema mapped_data_cdm; grant all on schema mapped_data_cdm to username"
```
## Load database schema

```bash
python ./utility_programs/load_schema_into_db.py
```
## Load vocabulary into database schema

```bash
python ./utility_programs/load_mapped_cdm_files_into_db.py
```

## Load mapped files into the database

```bash
./utility_programs/load_mapped_cdm_files_into_db.py`
```

## Prepared source specification

The prepared source is an intermediary format for mapping to OHDSI format.

Prefix nomenclature:

* "s_" source value - as represented source
* "m_" mapped value - a mapped value
* "k_" and key value which maps to another value in a table
* "i_" indicator field where "1" indicates true

Date formats:

* Date -- "2017-01-01"
* Date with local time -- "2012-02-03 05:34"
* Date with time zone -- "2011-08-01T15:45:00.000-05:00"

### source_person.csv

* s_person_id -- Source identifier for patient or person
* s_gender	-- Source gender for person
* m_gender -- {MALE, FEMALE, UNKNOWN}
* s_birth_datetime -- Date of birth can be either a date or a date time
* s_death_datetime -- Date of death can either be either a date or a date time
* s_race -- Source race value for person
* m_race -- {White, Black or African American, American Indian or Alaska native, . .}
* s_ethnicity -- Source ethnicity for person
* m_ethnicity -- Mapped value {Not Hispanic or Latino, Hispanic or Latino}
* k_location -- Not implemented 

### source_care_site.csv

* k_care_site -- 
* s_care_site_name -- 

### source_encounter.csv

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

### source_observation_period.csv

### source_encounter_coverage.csv

### source_condition.csv

### source_procedure.csv

### source_result.csv

### source_medication.csv

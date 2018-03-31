## Mapping to OMOP CDM

There is two step process to transform source CSV files into the OMOP/OHDSI 
common data model compatible CSV files. The first step is mapping the source file into the 
prepared source file format. Examples of files in the prepared source format 
can be seen in the `./test/input/` directory.

If files are formatted into prepared source then the script 
`transform_prepared_source_to_cdm.py` can be run to convert the input files 
into output files.

The scripts in the project require Python 3.6 and the following libraries:
 `sqlalchemy`, `psycopg2`, and `sqlparse`. To add these libraries to Python use
  `pip install psycopg2`.

## Download Athena concept tables

OHDSI makes available concept/vocabulary files. 
The concept/vocabulary files need to be downloaded from: http://www.ohdsi.org/web/athena/

The Athena web tools allows the user to select the needed vocabularies. After the 
request is submitted an email notification will be sent when the files are ready for download.
If you are working with claims data you will need a license for CPT codes and run a separate
process for including the CPT codes in the concept table.

To generate the CONCEPT.csv with the CPT codes you need to run a Java program. With newer versions of Java you need to
modify the command:

```bash
java --add-modules java.xml.ws -jar cpt4.jar 5
```

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

The `"json_map_directory"` points to the directory for the decompressed Athena concept files. 
The `"csv_input_directory"` points to where the source files are located and `csv_output_directory`
is the location where the OHDSI mapped files files will be written.  If you want
to load the files into a PostGreSQL database `connection_uri` can be set and `schema` is the specific 
database.

## Generating vocabulary JSON lookup

Before running the `./transform_prepared_source_to_cdm.py` vocabulary needs to generate JSON 
look up files. This process will convert the CSV files into separate focused vocabularies.

```bash
python ./utility_programs/generate_code_lookup_json.py -c hi_config.json
```

## Generate RxNorm mappings

```bash
# Generate mappings
python rxnorm_sourced_multum_mappings.py -c hi_config.json

python
```

## Mapping from source files to prepared_source

Currently there are two examples of mapped to the prepared_source CSV format. These
are for CSV extracts from a larger database. Mapping to prepared_source
CSV format can be made with any tool that supports CSV exporting.

## Mapping prepared_source to OHDSI CSV format

```bash
python transform_prepared_source_to_cdm.py -c hi_config.json
```

## Create database schema

```bash
psql ohdsi < echo "create schema mapped_data_cdm; grant all on schema mapped_data_cdm to username"
```

## Load database schema

```bash
python ./utility_programs/load_schema_into_db.py -c hi_config.json --ddl-file ..\schema\5.2\omop_cdm.sql \
 --index-file ..\schema\5.2\omop_cdm_indexes.sql
```
## Load vocabulary into database schema

```bash
python ./utility_programs/load_mapped_cdm_files_into_db.py -c hi_config.json
```

## Load mapped files into the database

```bash
python ./utility_programs/load_mapped_cdm_files_into_db.py -c hi_config.json
```

## Prepared source specification

The prepared source is an intermediary format for mapping to OHDSI format.

Prefix nomenclature:

* "s_" source value - as represented source
* "m_" mapped value - a mapped value
* "k_" a key value usually hash which maps to another value in a table
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

* k_care_site -- hashed key representing an organization
* s_care_site_name -- name of the care site

### source_encounter.csv

* s_encounter_id -- Source identifier for encounter
* s_person_id -- Source identifier for patient or person
* s_visit_start_datetime -- Start date or date time or admission date or time 
* s_visit_end_datetime -- End date or date time or discharge date or time
* s_visit_type -- Source of type of visit
* m_visit_type -- Type of visit {Inpatient, Outpatient, . .}
* k_care_site -- Linking key to location
* s_discharge_to -- Source value discharge disposition
* m_discharge_to -- 
* s_admitting_source -- Source value for admitting source
* m_admitting_source -- 

### source_observation_period.csv

* s_person_id -- Source identifier for patient or person
* s_start_observation_datetime -- Start period of patient observation 
* s_end_observation_datetime --  End period of patient observation

### source_encounter_coverage.csv

* s_person_id --
* s_encounter_id --
* s_start_payer_date -- in date format
* s_end_payer_date -- in date format
* s_payer_name -- 
* m_payer_name --  
* s_plan_name -- 
* m_plan_name -- 

### source_condition.csv

* s_person_id -- 
* s_encounter_id -- 
* s_start_condition_datetime -- 
* s_end_condition_datetime -- 
* s_condition_code -- 
* s_condition_code_type -- 
* m_condition_code_oid -- 
* s_sequence_id -- 
* s_rank -- 
* m_rank -- 
* s_condition_type -- 
* s_present_on_admission_indicator -- 
* i_exclude -- exclude from OHDSI mapper

### source_procedure.csv

* s_encounter_id -- Source identifier for patient of person
* s_person_id -- Source identifier for an encounter
* s_start_procedure_datetime
* s_end_procedure_datetime
* s_procedure_code
* s_procedure_code_type
* m_procedure_code_oid
* s_sequence_id
* s_rank
* m_rank
* i_exclude

### source_result.csv

* s_person_id
* s_encounter_id
* s_obtained_datetime
* s_type_name
* s_type_code
* m_type_code_oid
* s_result_text
* m_result_text
* s_result_numeric
* s_result_datetime
* s_result_code
* m_result_code_oid
* s_result_unit
* s_result_unit_code
* m_result_unit_code_oid
* s_result_numeric_lower
* s_result_numeric_upper
* i_exclude
                
### source_medication.csv

* s_person_id
* s_encounter_id
* s_drug_code
* s_drug_code_type -- Name of drug coding system {NDC, RxNorm} (not used in mapping)
* m_drug_code_oid -- OID for drug coding system
* s_drug_text -- Name of the drug
* s_start_medication_datetime -- Start of drug
* s_end_medication_datetime -- End of drug
* s_route
* m_route
* s_quantity
* s_dose -- The source dose form
* m_dose -- The mapped dose form
* s_dose_unit
* m_dose_unit
* s_status -- {Completed, Ordered, Canceled}
* s_drug_type -- How the drug was delivered {Inpatient, Office}
* m_drug_type -- In standard mapping
* i_exclude

```json
{
 "HOSPITAL_PHARMACY": "Inpatient administration",
 "INPATIENT_FLOOR_STOCK": "Inpatient administration",
 "RETAIL_PHARMACY": "Prescription dispensed in pharmacy",
 "UNKNOWN": "Prescription dispensed in pharmacy",
 "_NOT_VALUED": "Prescription written",
 "OFFICE": "Physician administered drug (identified from EHR observation)"
}
```

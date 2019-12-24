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

To generate the CONCEPT.csv with the CPT codes you need to run a Java program. With newer
versions of Java you need to modify the command:

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
The intermediary format is closer to how data is stored in clinical or administrative
database.

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

This file holds information about the person/patient.

* s_person_id -- Source identifier for patient or person
* s_gender	-- Source gender for person
* m_gender -- {MALE, FEMALE, UNKNOWN}
* s_birth_datetime -- Date of birth can be either a date or a date time
* s_death_datetime -- Date of death can either be either a date or a date time
* s_race -- Source race value for person
* m_race -- {White, Black or African American, American Indian or Alaska Native, . .}
* s_ethnicity -- Source ethnicity for person
* m_ethnicity -- Mapped value {Not Hispanic or Latino, Hispanic or Latino}
* k_location -- Not implemented 

### source_care_site.csv

This file holds information about the care site of an encounter. It is used to reference the encounter.

* k_care_site -- hashed key representing an organization
* s_care_site_name -- name of the care site

### source_encounter.csv

This file holds information about the encounter.

* s_encounter_id -- Source identifier for encounter
* s_person_id -- Source identifier for patient or person
* s_visit_start_datetime -- Start date or date time or admission date or time 
* s_visit_end_datetime -- End date or date time or discharge date or time
* s_visit_type -- Source of type of visit
* m_visit_type -- Type of visit {Inpatient, Outpatient, . .}
* k_care_site -- Linking key to location
* s_discharge_to -- Source value discharge disposition
* m_discharge_to -- Mapped value {}
* s_admitting_source -- Source value for admitting source
* m_admitting_source -- Mapped value {}

### source_observation_period.csv

This file holds information about the period of time that data is collected about the person and captured.

* s_person_id -- Source identifier for patient or person
* s_start_observation_datetime -- Start period of patient observation 
* s_end_observation_datetime --  End period of patient observation

### source_encounter_coverage.csv

This file holds information about how the encounter is covered by a payer.

* s_person_id -- Source identifier for patient or person
* s_encounter_id --  -- Source identifier for an encounter
* s_start_payer_date -- in date format
* s_end_payer_date -- in date format
* s_payer_name -- the payer name
* m_payer_name --  the mapped payer name
* s_plan_name --  the plan name
* m_plan_name --  the mapped plan name

### source_condition.csv

This file holds information about the recorded conditions for a person and/or encounter.

* s_person_id -- Source identifier for patient or person
* s_encounter_id -- Source identifier for an encounter
* s_start_condition_datetime -- The condition start time
* s_end_condition_datetime --  The condition end time
* s_condition_code -- The actual condition code
* s_condition_code_type -- Source condition
* m_condition_code_oid -- {ICD9: 2.16.840.1.113883.6.103, ICD10: 2.16.840.1.113883.6.90}
* s_sequence_id -- The order of the diagnosis codes
* s_rank -- The rank of the test
* m_rank -- The mapped rank {Primary, Secondary}
* s_condition_type -- Admit, discharge, problem list
* s_present_on_admission_indicator -- For billing purposes indicates whether the
* i_exclude -- exclude row from the mapper

### source_procedure.csv

This file holds information the recorded procedure for a person and/or encounter.

* s_encounter_id -- Source identifier for an encounter
* s_person_id -- Source identifier for patient of person
* s_start_procedure_datetime -- The start of the procedure
* s_end_procedure_datetime -- The end of the procedure
* s_procedure_code -- The code for the procedure
* s_procedure_code_type  -- The type of procedure code {ICD10:PCS, CPT}
* m_procedure_code_oid -- The OID for the coding system: CPT: 	"2.16.840.1.113883.6.12"
* s_sequence_id -- The procedure order as stored in the source system
* s_rank -- The rank of the procedure:
* m_rank -- The mapped rank of the procedure
* i_exclude -- exclude row from the mapper

### source_result.csv

This file holds all recorded measurements for a person and/or encounter.

* s_person_id -- Source identifier for patient or person
* s_encounter_id -- Source identifier for an encounter
* s_obtained_datetime -- The date the result was obtained or measured
* s_name -- The text description of lab test / measurement name, e.g., Hemoglobin A1C
* s_code -- The code for the lab test/measurement, e.g., "4548-4"
* s_code_type -- The type of code for example LOINC
* m_type_code_oid -- The OID for the lab test/measurement, e.g., "2.16.840.1.113883.6.1"
* s_result_text -- The text associated with the result., e.g., Above normal
* m_result_text -- A mapped name of the result of the lab
* s_result_numeric -- The numeric result of the lab test
* s_result_datetime -- If the measurement has a date associated with it, for example, date of last
* s_result_code -- If the result was a specific coded result
* m_result_code_oid -- The OID For the coding system
* s_result_unit -- The units of the measurement
* s_result_unit_code -- A code for the units
* m_result_unit_code_oid -- The OID for the coded unites
* s_result_numeric_lower -- Numeric lower limit for normal
* s_result_numeric_upper -- Numeric upper limit for normal
* i_exclude -- exclude row from the mapper
                
### source_medication.csv

This file holds all medications orders for a person and/or encounter

* s_person_id -- Source identifier for patient or person
* s_encounter_id -- Source identifier for an encounter
* s_drug_code -- The drug code for the medication
* s_drug_code_type -- Name of drug coding system {NDC, RxNorm} (not used in mapping)
* m_drug_code_oid -- OID for drug coding system
* s_drug_text -- Name of the drug
* s_drug_alternative_text -- Alternative drug name (could be a generic or brand name)
* s_start_medication_datetime -- Start of medication date time
* s_end_medication_datetime -- End of medication date time
* s_route -- Source route {Oral, Intravenous}
* m_route -- Mapped route to standard
* s_quantity == The quantity dispensed to the person
* s_dose -- The source dose form
* m_dose -- The mapped dose form to standard unites
* s_dose_unit == Source unit of dose
* m_dose_unit == Mapped unit of dose
* s_status -- Status of the medication order{Completed, Ordered, Canceled}
* s_drug_type -- Source how the drug was delivered {Inpatient, Office}
* m_drug_type -- Mapped how the drug was delivered
* i_exclude

/* Source table definitions from:

https://github.com/OHDSI/CommonDataModel/tree/master/PostgreSQL

 */


CREATE TABLE  person
    (
     person_id						INTEGER		NOT NULL ,
     gender_concept_id				INTEGER		NOT NULL ,
     year_of_birth					INTEGER		NOT NULL ,
     month_of_birth					INTEGER		NULL,
     day_of_birth					INTEGER		NULL,
	   time_of_birth					VARCHAR(10)	NULL,
     race_concept_id				INTEGER		NOT NULL,
     ethnicity_concept_id			INTEGER		NOT NULL,
     location_id					INTEGER		NULL,
     provider_id					INTEGER		NULL,
     care_site_id					INTEGER		NULL,
     person_source_value			VARCHAR(50) NULL,
     gender_source_value			VARCHAR(50) NULL,
	   gender_source_concept_id		INTEGER		NULL,
     race_source_value				VARCHAR(50) NULL,
	   race_source_concept_id			INTEGER		NULL,
     ethnicity_source_value			VARCHAR(50) NULL,
	   ethnicity_source_concept_id	INTEGER		NULL
    )
;


CREATE TABLE visit_occurrence
    (
     visit_occurrence_id			INTEGER			NOT NULL ,
     person_id						INTEGER			NOT NULL ,
     visit_concept_id				INTEGER			NOT NULL ,
	 visit_start_date				DATE			NOT NULL ,
	 visit_start_time				VARCHAR(10)		NULL ,
     visit_end_date					DATE			NOT NULL ,
	 visit_end_time					VARCHAR(10)		NULL ,
	 visit_type_concept_id			INTEGER			NOT NULL ,
	 provider_id					INTEGER			NULL,
     care_site_id					INTEGER			NULL,
     visit_source_value				VARCHAR(50)		NULL,
	 visit_source_concept_id		INTEGER			NULL
    )
;


CREATE TABLE condition_occurrence
    (
     condition_occurrence_id		INTEGER			NOT NULL ,
     person_id						INTEGER			NOT NULL ,
     condition_concept_id			INTEGER			NOT NULL ,
     condition_start_date			DATE			NOT NULL ,
     condition_end_date				DATE			NULL ,
     condition_type_concept_id		INTEGER			NOT NULL ,
     stop_reason					VARCHAR(20)		NULL ,
     provider_id					INTEGER			NULL ,
     visit_occurrence_id			INTEGER			NULL ,
     condition_source_value			VARCHAR(50)		NULL ,
	 condition_source_concept_id	INTEGER			NULL
    )
;

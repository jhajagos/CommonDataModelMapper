/* Make some basic changes to the schema to support longer fields for some source fields */

alter table visit_occurrence alter column visit_source_value type varchar(1023);

alter table visit_occurrence alter column admitting_source_value type varchar(1023);

alter table visit_occurrence alter column discharge_to_source_value type varchar(1023);

alter table drug_exposure alter column drug_source_value type varchar(1023);

alter table measurement alter column value_source_value type varchar(1023);

alter table measurement alter column  measurement_source_value type varchar(1023);

alter table observation alter column value_as_string type varchar(1023);

alter table visit_detail alter column visit_detail_source_value type varchar(1023);



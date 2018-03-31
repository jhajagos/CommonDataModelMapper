/* Make some basic changes to the schema to support longer fields for some source fields */

alter table visit_occurrence alter column visit_source_value type varchar(511);

alter table drug_exposure alter column drug_source_value type varchar(511);

alter table measurement alter column value_source_value type varchar(511);

alter table observation alter column value_as_string type varchar(511);


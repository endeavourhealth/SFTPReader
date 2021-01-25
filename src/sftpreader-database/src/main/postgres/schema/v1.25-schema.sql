alter table log.batch
add extract_date timestamp without time zone,
add extract_cutoff timestamp without time zone;

alter table log.batch_split
add has_patient_data boolean NULL;
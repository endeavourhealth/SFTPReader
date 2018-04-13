

-- drop columns off log.batch_file since they're not needed
alter table log.batch_file
drop column local_size_bytes;

alter table log.batch_file
drop column requires_decryption;

alter table log.batch_file
drop column is_decrypted;

alter table log.batch_file
drop column decrypt_date;

alter table log.batch_file
drop column decrypted_filename;

alter table log.batch_file
drop column decrypted_size_bytes;

-- new table for TPP orgs

CREATE TABLE configuration.tpp_organisation_map
(
  ods_code character varying NOT NULL,
  name character varying NOT NULL,
  CONSTRAINT configuration_tpporganisationmap_guid_pk PRIMARY KEY (ods_code)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE configuration.tpp_organisation_map
  OWNER TO postgres;
GRANT ALL ON TABLE configuration.tpp_organisation_map TO postgres;

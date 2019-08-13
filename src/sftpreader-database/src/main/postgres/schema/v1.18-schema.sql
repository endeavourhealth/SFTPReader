CREATE TABLE configuration.adastra_organisation_map (
  ods_code varchar(255) NOT NULL,
  file_name_org_code varchar(255) NOT NULL,
  configuration_id varchar(100) NOT NULL,
  CONSTRAINT adastra_organisation_map_pk PRIMARY KEY (ods_code),
  CONSTRAINT adastra_organisation_map_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration.configuration (configuration_id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX ix_adastra_organisation_map_configuration ON configuration.adastra_organisation_map (configuration_id, file_name_org_code);

ALTER TABLE configuration.adastra_organisation_map
  OWNER TO postgres;
GRANT ALL ON TABLE configuration.adastra_organisation_map TO postgres;


ALTER TABLE configuration.interface_type
ADD data_frequency_days int not null default 1;


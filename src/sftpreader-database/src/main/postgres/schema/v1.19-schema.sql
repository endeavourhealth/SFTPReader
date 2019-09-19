CREATE TABLE configuration.tpp_organisation_gms_registration_map (
  organisation_id		varchar(20),
  patient_id bigint,
  gms_organisation_id	varchar(20),
  CONSTRAINT tpp_organisation_gms_registration_map_pk PRIMARY KEY (organisation_id, patient_id, gms_organisation_id)
)
WITH (
       OIDS=FALSE
);

CREATE INDEX tpp_organisation_gms_registration_map_organisation_id_ix
  ON configuration.tpp_organisation_gms_registration_map
    (organisation_id);

ALTER TABLE configuration.tpp_organisation_gms_registration_map
  OWNER TO postgres;
GRANT ALL ON TABLE configuration.tpp_organisation_gms_registration_map TO postgres;



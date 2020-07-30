--  Scripts for SD-89 TPP Transform SRCtv3ToSnomed
use publisher_common;

DROP TABLE IF EXISTS tpp_ctv3_to_snomed;

CREATE TABLE  publisher_common.tpp_ctv3_to_snomed (
  ctv3_code varchar(5) NOT NULL,
  snomed_concept_id bigint(20) NOT NULL,
  dt_last_updated datetime NOT NULL,
  CONSTRAINT tpp_ctv3_to_snomed_ctv3_code_pk PRIMARY KEY (ctv3_code )
);


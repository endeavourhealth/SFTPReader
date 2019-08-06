CREATE TABLE log.configuration_polling_attempt
(
	configuration_id character varying(100) NOT NULL,
	attempt_started timestamp without time zone NOT NULL,
	attempt_finished timestamp without time zone NOT NULL,
	exception_text varchar,
	  files_downloaded int,
  batches_completed int,
  batch_splits_notified_ok int,
  batch_splits_notified_failure int,
	CONSTRAINT cconfiguration_polling_attempt_pk PRIMARY KEY (configuration_id, attempt_started),
	CONSTRAINT configuration_polling_attempt_fk foreign key (configuration_id) references configuration.configuration (configuration_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE log.configuration_polling_attempt
  OWNER TO postgres;
GRANT ALL ON TABLE log.configuration_polling_attempt TO postgres;

CREATE TABLE configuration.configuration_paused_notifying
(
	configuration_id character varying(100) NOT NULL,
	dt_paused timestamp not null default (date_trunc('second', now()::timestamp)),
	CONSTRAINT pk PRIMARY KEY (configuration_id)
)
WITH (
  OIDS=FALSE
);

ALTER TABLE configuration.configuration_paused_notifying OWNER TO postgres;

GRANT ALL ON TABLE configuration.configuration_paused_notifying TO postgres;




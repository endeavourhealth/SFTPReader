CRETE DATABASE sftp_reader;
USE sftp_reader;

DROP TABLE IF EXISTS configuration_paused_notifying;
DROP TABLE IF EXISTS configuration_polling_attempt;
DROP TABLE IF EXISTS unknown_file;
DROP TABLE IF EXISTS notification_message;
DROP TABLE IF EXISTS batch_split;
DROP TABLE IF EXISTS batch_file;
DROP TABLE IF EXISTS batch;
DROP TABLE IF EXISTS tpp_organisation_map;
DROP TABLE IF EXISTS instance_configuration;
DROP TABLE IF EXISTS instance;
DROP TABLE IF EXISTS emis_organisation_map;
DROP TABLE IF EXISTS adastra_organisation_map;
DROP TABLE IF EXISTS configuration_eds;
DROP TABLE IF EXISTS configuration_sftp;
DROP TABLE IF EXISTS configuration_pgp;
DROP TABLE IF EXISTS configuration_kvp;
DROP TABLE IF EXISTS configuration;
DROP TABLE IF EXISTS interface_file_type;
DROP TABLE IF EXISTS interface_type;
DROP TABLE IF EXISTS tpp_organisation_gms_registration_map;



CREATE TABLE interface_type
(
  interface_type_id integer NOT NULL,
  interface_type_name varchar(1000) NOT NULL,
  data_frequency_days int not null COMMENT 'how often we expect data in days',
  CONSTRAINT interfacetype_interfacetypeid_pk PRIMARY KEY (interface_type_id),
  CONSTRAINT interfacetype_interfacetypename_uq UNIQUE (interface_type_name)
);

CREATE TABLE interface_file_type
(
  interface_type_id integer NOT NULL,
  file_type_identifier varchar(1000) NOT NULL,
  CONSTRAINT interfacefiletype_ifacetypeid_ftypeidentifier_pk PRIMARY KEY (interface_type_id, file_type_identifier),
  CONSTRAINT interfacefiletype_interfacetypeid_fk FOREIGN KEY (interface_type_id)
      REFERENCES interface_type (interface_type_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE configuration
(
  configuration_id varchar(255) NOT NULL,
  interface_type_id int NOT NULL,
  poll_frequency_seconds int NOT NULL,
  local_root_path varchar(1000) NOT NULL,
  configuration_friendly_name varchar(100) NOT NULL,
  software_content_type varchar(100) NOT NULL,
  software_version varchar(100) NOT NULL,
  CONSTRAINT configuration_pk PRIMARY KEY (configuration_id),
  CONSTRAINT configuration_interfacetypeid_fk FOREIGN KEY (interface_type_id)
      REFERENCES interface_type (interface_type_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT configurationfriendlyname_uq UNIQUE (configuration_friendly_name),
  CONSTRAINT configurationid_interfacetypeid_uq UNIQUE (configuration_id, interface_type_id)
);

CREATE TABLE configuration_kvp
(
  configuration_id varchar(100) NOT NULL,
  `key` varchar(100) NOT NULL,
  value varchar(1000) NOT NULL,
  CONSTRAINT configuration_configurationid_key_pk PRIMARY KEY (configuration_id, `key`),
  CONSTRAINT configurationkvp_configurationid_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE configuration_pgp
(
  configuration_id varchar(100) NOT NULL,
  file_extension_filter varchar(100) NOT NULL,
  sender_public_key mediumtext NOT NULL,
  recipient_public_key mediumtext NOT NULL,
  recipient_private_key mediumtext NOT NULL,
  recipient_private_key_password varchar(1000) NOT NULL,
  CONSTRAINT configurationpgp_configurationid_pk PRIMARY KEY (configuration_id),
  CONSTRAINT configurationpgp_configurationid_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE configuration_sftp
(
  configuration_id varchar(100) NOT NULL,
  hostname varchar(100) NOT NULL,
  port integer NOT NULL,
  remote_path varchar(1000) NOT NULL,
  username varchar(100) NOT NULL,
  client_public_key mediumtext NOT NULL,
  client_private_key mediumtext NOT NULL,
  client_private_key_password varchar(1000) NOT NULL,
  host_public_key mediumtext NOT NULL,
  CONSTRAINT configurationsftp_configurationid_pk PRIMARY KEY (configuration_id),
  CONSTRAINT configurationsftp_configurationid_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE configuration_eds
(
  single_row_lock boolean NOT NULL,
  eds_url varchar(1000) NOT NULL,
  use_keycloak boolean NOT NULL,
  keycloak_token_uri varchar(500),
  keycloak_realm varchar(100),
  keycloak_username varchar(100),
  keycloak_password varchar(100),
  keycloak_clientid varchar(100),
  temp_directory varchar(255),
  shared_storage_path varchar(255),
  CONSTRAINT configuration_eds_singlerowlock_pk PRIMARY KEY (single_row_lock),
  CONSTRAINT configuration_eds_singlerowlock_ck CHECK (single_row_lock = true)
);


CREATE TABLE emis_organisation_map
(
  guid varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  ods_code varchar(255) NOT NULL,
  CONSTRAINT configuration_emisorganisationmap_guid_pk PRIMARY KEY (guid)
);

CREATE INDEX emisorganisationmap_guid_ix
  ON emis_organisation_map (guid);

CREATE TABLE instance
(
  instance_name varchar(100) NOT NULL,
  hostname varchar(500),
  http_management_port integer,
  last_config_get_date datetime,
  CONSTRAINT configuration_instance_instancename_pk PRIMARY KEY (instance_name)
);

CREATE TABLE instance_configuration
(
  instance_name varchar(100) NOT NULL,
  configuration_id varchar(100) NOT NULL,
  CONSTRAINT instanceconfiguration_instancename_configid_pk PRIMARY KEY (instance_name, configuration_id),
  CONSTRAINT instanceconfiguration_configurationid_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT instanceconfiguration_instancename_fk FOREIGN KEY (instance_name)
      REFERENCES instance (instance_name) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT instanceconfiguration_configurationid_uq UNIQUE (configuration_id)
);

CREATE TABLE tpp_organisation_map
(
  ods_code varchar(255) NOT NULL,
  name varchar(255) NOT NULL,
  CONSTRAINT tpporganisationmap_guid_pk PRIMARY KEY (ods_code)
);

CREATE TABLE batch
(
  batch_id int NOT NULL,
  configuration_id varchar(100) NOT NULL,
  interface_type_id integer NOT NULL,
  batch_identifier varchar(500) NOT NULL,
  local_relative_path varchar(1000) NOT NULL,
  insert_date datetime,
  sequence_number integer,
  is_complete boolean NOT NULL,
  complete_date datetime null,
  CONSTRAINT batch_filesetid_pk PRIMARY KEY (batch_id),
  CONSTRAINT batch_configurationid_interfacetypeid_fk FOREIGN KEY (configuration_id, interface_type_id)
      REFERENCES configuration (configuration_id, interface_type_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT batch_configurationid_batchid_uq UNIQUE (configuration_id, batch_id),
  -- CONSTRAINT batch_configurationid_batchidentifier_uq UNIQUE (configuration_id, batch_identifier), now allow multiple batches with the same ID
  CONSTRAINT batch_configurationid_sequencenumber_uq UNIQUE (configuration_id, sequence_number)
);

CREATE INDEX ix_batch_configuration_identifer on batch (configuration_id, batch_identifier);

ALTER TABLE batch MODIFY COLUMN batch_id INT auto_increment;

CREATE TABLE batch_file
(
  batch_file_id int NOT NULL,
  batch_id integer NOT NULL,
  interface_type_id integer NOT NULL,
  file_type_identifier varchar(1000) NOT NULL,
  insert_date datetime,
  filename varchar(1000) NOT NULL,
  remote_created_date datetime null,
  remote_size_bytes bigint NOT NULL,
  is_downloaded boolean NOT NULL DEFAULT false,
  download_date datetime null,
  is_deleted boolean default false,
  CONSTRAINT batchfile_batchfileid_pk PRIMARY KEY (batch_file_id),
  CONSTRAINT batchfile_batchid_fk FOREIGN KEY (batch_id)
      REFERENCES batch (batch_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT batchfile_batchid_filename_uq UNIQUE (batch_id, filename)
);

ALTER TABLE batch_file MODIFY COLUMN batch_file_id INT auto_increment;

CREATE TABLE batch_split
(
  batch_split_id int NOT NULL,
  batch_id integer NOT NULL,
  configuration_id varchar(100) NOT NULL,
  local_relative_path varchar(1000) NOT NULL,
  organisation_id varchar(100) NOT NULL,
  have_notified boolean NOT NULL DEFAULT false,
  notification_date datetime null,
  is_bulk boolean null COMMENT 'whether this extract is known to contain a bulk extract',
  CONSTRAINT batchsplit_batchsplitid_pk PRIMARY KEY (batch_split_id),
  CONSTRAINT batchsplit_configurationid_batchid_fk FOREIGN KEY (configuration_id, batch_id)
      REFERENCES batch (configuration_id, batch_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT log_batchsplit_configurationid_batchid_batchsplitid_uq UNIQUE (configuration_id, batch_id, batch_split_id)
);

ALTER TABLE batch_split MODIFY COLUMN batch_split_id INT auto_increment;

create index log_batchsplit_configurationid_localrelativepath on batch_split (configuration_id, local_relative_path);

CREATE INDEX batch_split_batch_id ON batch_split (batch_id);

CREATE TABLE notification_message
(
  notification_message_id int NOT NULL,
  batch_id integer NOT NULL,
  batch_split_id integer NOT NULL,
  configuration_id varchar(100) NOT NULL,
  message_uuid char(36) NOT NULL,
  `timestamp` datetime NULL,
  outbound mediumtext,
  inbound mediumtext,
  was_success boolean NOT NULL,
  error_text mediumtext,
  CONSTRAINT notificationmessage_notificationmessageid_pk PRIMARY KEY (notification_message_id),
  CONSTRAINT notificationmessage_configurationid_batchid_batchsplitid_fk FOREIGN KEY (configuration_id, batch_id, batch_split_id)
      REFERENCES batch_split (configuration_id, batch_id, batch_split_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT notificationmessage_messageuuid_uq UNIQUE (message_uuid)
);

ALTER TABLE notification_message MODIFY COLUMN notification_message_id INT auto_increment;

CREATE TABLE unknown_file
(
  unknown_file_id int NOT NULL,
  configuration_id varchar(100) NOT NULL,
  insert_date datetime null,
  filename varchar(1000) NOT NULL,
  remote_created_date datetime NULL,
  remote_size_bytes bigint NOT NULL,
  CONSTRAINT unknownfile_unknownfileid_pk PRIMARY KEY (unknown_file_id),
  CONSTRAINT unknownfile_configurationid_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

ALTER TABLE unknown_file MODIFY COLUMN unknown_file_id INT auto_increment;

CREATE TABLE configuration_polling_attempt (
  configuration_id varchar(100) NOT NULL,
  attempt_started datetime NOT NULL,
  attempt_finished datetime NOT NULL,
  exception_text mediumtext,
  files_downloaded int,
  batches_completed int,
  batch_splits_notified_ok int,
  batch_splits_notified_failure int,
	CONSTRAINT configuration_polling_attempt_pk PRIMARY KEY (configuration_id, attempt_started),
	CONSTRAINT configuration_polling_attempt_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE adastra_organisation_map (
  ods_code varchar(255) NOT NULL,
  file_name_org_code varchar(255) NOT NULL,
  configuration_id varchar(100) NOT NULL,
  CONSTRAINT adastra_organisation_map_pk PRIMARY KEY (ods_code),
  CONSTRAINT adastra_organisation_map_fk FOREIGN KEY (configuration_id)
      REFERENCES configuration (configuration_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE INDEX ix_adastra_organisation_map_configuration ON adastra_organisation_map (configuration_id, file_name_org_code);


CREATE TABLE tpp_organisation_gms_registration_map (
  organisation_id		varchar(20),
  patient_id bigint,
  gms_organisation_id	varchar(20),
  CONSTRAINT tpp_organisation_gms_registration_map_pk PRIMARY KEY (organisation_id, patient_id, gms_organisation_id)
);

CREATE INDEX tpp_organisation_gms_registration_map_organisation_id_ix
  ON tpp_organisation_gms_registration_map
    (organisation_id);

CREATE TABLE configuration_paused_notifying (
	`configuration_id` varchar(255) NOT NULL,
    dt_paused datetime DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`configuration_id`)
);


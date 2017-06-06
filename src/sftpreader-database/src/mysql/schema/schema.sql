CREATE TABLE interface_type (
	interface_type_id integer NOT NULL,
	interface_type_name varchar(1000) NOT NULL,

	CONSTRAINT interfacetype_interfacetypeid_pk PRIMARY KEY (interface_type_id),
	CONSTRAINT interfacetype_interfacetypename_uq UNIQUE (interface_type_name)
);

CREATE TABLE interface_file_type (
	interface_type_id integer NOT NULL,
	file_type_identifier varchar(1000) NOT NULL,

	CONSTRAINT interfacefiletype_ifacetypeid_ftypeidentifier_pk PRIMARY KEY (interface_type_id, file_type_identifier),
	CONSTRAINT interfacefiletype_interfacetypeid_fk FOREIGN KEY (interface_type_id) REFERENCES interface_type (interface_type_id)
);

CREATE TABLE configuration (
	configuration_id varchar(100) NOT NULL,
	interface_type_id integer NOT NULL,
	poll_frequency_seconds integer NOT NULL,
	local_root_path_prefix varchar(500) NOT NULL,
	local_root_path varchar(500) NOT NULL,
	configuration_friendly_name varchar(100) NOT NULL,

	CONSTRAINT configuration_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configuration_interfacetypeid_fk FOREIGN KEY (interface_type_id) REFERENCES interface_type(interface_type_id),
	CONSTRAINT configuration_configurationfriendlyname_uq UNIQUE (configuration_friendly_name),
	CONSTRAINT configuration_configurationid_interfacetypeid_uq UNIQUE (configuration_id, interface_type_id),
	CONSTRAINT configuration_localrootpaths_uq UNIQUE (local_root_path_prefix, local_root_path)
);

CREATE TABLE configuration_kvp (
	configuration_id varchar(100) NOT NULL,
	configuration_key varchar(100) NOT NULL,
	configuration_value varchar(1000) NOT NULL,

	CONSTRAINT configurationkvp_configurationid_configurationkey_pk PRIMARY KEY (configuration_id, configuration_key),
	CONSTRAINT configurationkvp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE configuration_pgp (
	configuration_id varchar(100) NOT NULL,
	file_extension_filter varchar(100) NOT NULL,
	sender_public_key text NOT NULL,
	recipient_public_key text NOT NULL,
	recipient_private_key text NOT NULL,
	recipient_private_key_password varchar(1000) NOT NULL,

	CONSTRAINT configurationpgp_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configurationpgp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE configuration_sftp (
	configuration_id varchar(100) NOT NULL,
	hostname varchar(100) NOT NULL,
	port integer NOT NULL,
	remote_path varchar(1000) NOT NULL,
	username varchar(100) NOT NULL,
	client_public_key text NOT NULL,
	client_private_key text NOT NULL,
	client_private_key_password varchar(1000) NOT NULL,
	host_public_key text NOT NULL,

	CONSTRAINT configurationsftp_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configurationsftp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE eds_endpoint (
	single_row_lock boolean NOT NULL,
	eds_url varchar(1000) NOT NULL,
	software_content_type varchar(100) NOT NULL,
	software_version varchar(100) NOT NULL,
	use_keycloak boolean NOT NULL,
	keycloak_token_uri varchar(500),
	keycloak_realm varchar(100),
	keycloak_username varchar(100),
	keycloak_password varchar(100),
	keycloak_clientid varchar(100),

	CONSTRAINT edsendpoint_singlerowlock_pk PRIMARY KEY (single_row_lock)
);

CREATE TABLE emis_organisation_map (
	guid varchar(100) NOT NULL,
	name varchar(1000) NOT NULL,
	ods_code varchar(100) NOT NULL,

	CONSTRAINT emisorganisationmap_guid_pk PRIMARY KEY (guid)
);

CREATE INDEX configuration_emisorganisationmap_guid_ix ON emis_organisation_map (guid);

CREATE TABLE instance (
	instance_name varchar(100) NOT NULL,
	hostname varchar(500),
	http_management_port integer,
	last_config_get_date datetime,

	CONSTRAINT instance_instancename_pk PRIMARY KEY (instance_name)
);

CREATE TABLE instance_configuration (
	instance_name varchar(100) NOT NULL,
	configuration_id varchar(100) NOT NULL,

	CONSTRAINT instanceconfiguration_instancename_configid_pk PRIMARY KEY (instance_name, configuration_id),
	CONSTRAINT instanceconfiguration_configurationid_uq UNIQUE (configuration_id),
	CONSTRAINT instanceconfiguration_instancename_fk FOREIGN KEY (instance_name) REFERENCES instance (instance_name),
	CONSTRAINT instanceconfiguration_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE slack_endpoint (
	single_row_lock boolean NOT NULL,
	enabled boolean NOT NULL,
	slack_url varchar(1000) NOT NULL,

	CONSTRAINT slackendpoint_singlerowlock_pk PRIMARY KEY (single_row_lock)
);

CREATE TABLE batch (
	batch_id integer NOT NULL,
	configuration_id varchar(100) NOT NULL,
	interface_type_id integer NOT NULL,
	batch_identifier varchar(500) NOT NULL,
	local_relative_path varchar(1000) NOT NULL,
	insert_date datetime NOT NULL,
	sequence_number integer,
	is_complete boolean DEFAULT false NOT NULL,
	complete_date datetime,

	CONSTRAINT batch_filesetid_pk PRIMARY KEY (batch_id),
	CONSTRAINT batch_configurationid_batchid_uq UNIQUE (configuration_id, batch_id),
	CONSTRAINT batch_configurationid_batchidentifier_uq UNIQUE (configuration_id, batch_identifier),
	CONSTRAINT batch_configurationid_sequencenumber_uq UNIQUE (configuration_id, sequence_number),
	CONSTRAINT batch_configurationid_interfacetypeid_fk FOREIGN KEY (configuration_id, interface_type_id) REFERENCES configuration (configuration_id, interface_type_id)
);

CREATE TABLE batch_file (
	batch_file_id integer NOT NULL,
	batch_id integer NOT NULL,
	interface_type_id integer NOT NULL,
	file_type_identifier varchar(1000) NOT NULL,
	insert_date datetime NOT NULL,
	filename varchar(1000) NOT NULL,
	remote_created_date datetime NOT NULL,
	remote_size_bytes bigint NOT NULL,
	is_downloaded boolean DEFAULT false NOT NULL,
	download_date datetime,
	local_size_bytes bigint,
	requires_decryption boolean NOT NULL,
	is_decrypted boolean,
	decrypt_date datetime,
	decrypted_filename varchar(1000),
	decrypted_size_bytes bigint,

	CONSTRAINT batchfile_batchfileid_pk PRIMARY KEY (batch_file_id),
	CONSTRAINT batchfile_batchid_decryptedfilename_uq UNIQUE (batch_id, decrypted_filename),
	CONSTRAINT batchfile_batchid_filename_uq UNIQUE (batch_id, filename),
	CONSTRAINT batchfile_batchid_filetypeidentifier_uq UNIQUE (batch_id, file_type_identifier),
	CONSTRAINT batchfile_batchid_fk FOREIGN KEY (batch_id) REFERENCES batch(batch_id),
	CONSTRAINT batchfile_interfacetypeid_filetypeidentifier_fk FOREIGN KEY (interface_type_id, file_type_identifier) REFERENCES interface_file_type (interface_type_id, file_type_identifier)
);

CREATE TABLE batch_split (
	batch_split_id integer NOT NULL,
	batch_id integer NOT NULL,
	configuration_id varchar(100) NOT NULL,
	local_relative_path varchar(1000) NOT NULL,
	organisation_id varchar(100) NOT NULL,
	have_notified boolean DEFAULT false NOT NULL,
	notification_date datetime,

	CONSTRAINT batchsplit_batchsplitid_pk PRIMARY KEY (batch_split_id),
	CONSTRAINT batchsplit_configurationid_batchid_batchsplitid_uq UNIQUE (configuration_id, batch_id, batch_split_id),
	CONSTRAINT batchsplit_configurationid_batchid_fk FOREIGN KEY (configuration_id, batch_id) REFERENCES batch (configuration_id, batch_id)
);

CREATE TABLE error_digest (
	error_digest_id integer NOT NULL,
	error_count integer NOT NULL,
	last_log_date datetime NOT NULL,
	log_class varchar(1000) NOT NULL,
	log_method varchar(1000) NOT NULL,
	log_message varchar(1000) NOT NULL,
	exception varchar(8000) NOT NULL,

	CONSTRAINT errordigest_errordigestid_pk PRIMARY KEY (error_digest_id)
);

CREATE TABLE notification_message (
	notification_message_id integer NOT NULL,
	batch_id integer NOT NULL,
	batch_split_id integer NOT NULL,
	configuration_id varchar(100) NOT NULL,
	message_uuid varchar(100) NOT NULL,
	insert_date datetime NOT NULL,
	outbound text NOT NULL,
	inbound text,
	was_success boolean NOT NULL,
	error_text text,

	CONSTRAINT notificationmessage_notificationmessageid_pk PRIMARY KEY (notification_message_id),
	CONSTRAINT notificationmessage_messageuuid_uq UNIQUE (message_uuid),
	CONSTRAINT notificationmessage_configurationid_batchid_batchsplitid_fk FOREIGN KEY (configuration_id, batch_id, batch_split_id) REFERENCES batch_split (configuration_id, batch_id, batch_split_id)
);

CREATE TABLE unknown_file (
	unknown_file_id integer NOT NULL AUTO_INCREMENT,
	configuration_id varchar(100) NOT NULL,
	insert_date datetime NOT NULL,
	filename varchar(1000) NOT NULL,
	remote_created_date datetime NOT NULL,
	remote_size_bytes bigint NOT NULL,

	CONSTRAINT unknownfile_unknownfileid_pk PRIMARY KEY (unknown_file_id),
	CONSTRAINT unknownfile_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

insert into interface_type (interface_type_id, interface_type_name) values
(1, 'EMIS-EXTRACT-SERVICE-5-1');

insert into interface_file_type (interface_type_id, file_type_identifier) values
(1, 'Admin_Location'),
(1, 'Admin_Organisation'),
(1, 'Admin_OrganisationLocation'),
(1, 'Admin_Patient'),
(1, 'Admin_UserInRole'),
(1, 'Agreements_SharingOrganisation'),
(1, 'Appointment_Session'),
(1, 'Appointment_SessionUser'),
(1, 'Appointment_Slot'),
(1, 'Audit_RegistrationAudit'),
(1, 'Audit_PatientAudit'),
(1, 'CareRecord_Consultation'),
(1, 'CareRecord_Diary'),
(1, 'CareRecord_Observation'),
(1, 'CareRecord_ObservationReferral'),
(1, 'CareRecord_Problem'),
(1, 'Coding_ClinicalCode'),
(1, 'Coding_DrugCode'),
(1, 'Prescribing_DrugRecord'),
(1, 'Prescribing_IssueRecord');

CREATE TABLE interface_type (
	interface_type_id integer NOT NULL,
	interface_type_name character varying(1000) NOT NULL,

	CONSTRAINT interfacetype_interfacetypeid_pk PRIMARY KEY (interface_type_id),
	CONSTRAINT interfacetype_interfacetypename_uq UNIQUE (interface_type_name),
	CONSTRAINT interfacetype_interfacetypename_ck CHECK ((char_length(btrim((interface_type_name)::text)) > 0))
);

CREATE TABLE interface_file_type (
	interface_type_id integer NOT NULL,
	file_type_identifier character varying(1000) NOT NULL,

	CONSTRAINT interfacefiletype_ifacetypeid_ftypeidentifier_pk PRIMARY KEY (interface_type_id, file_type_identifier),
	CONSTRAINT interfacefiletype_filetypeidentifier_ck CHECK ((char_length(btrim((file_type_identifier)::text)) > 0)),
	CONSTRAINT interfacefiletype_interfacetypeid_fk FOREIGN KEY (interface_type_id) REFERENCES interface_type (interface_type_id)
);

CREATE TABLE configuration (
	configuration_id character varying(100) NOT NULL,
	interface_type_id integer NOT NULL,
	poll_frequency_seconds integer NOT NULL,
	local_root_path_prefix character varying(1000) NOT NULL,
	local_root_path character varying(1000) NOT NULL,
	configuration_friendly_name character varying(100) NOT NULL,

	CONSTRAINT configuration_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configuration_interfacetypeid_fk FOREIGN KEY (interface_type_id) REFERENCES interface_type(interface_type_id),
	CONSTRAINT configuration_configurationfriendlyname_uq UNIQUE (configuration_friendly_name),
	CONSTRAINT configuration_configurationid_interfacetypeid_uq UNIQUE (configuration_id, interface_type_id),
	CONSTRAINT configuration_localrootpaths_uq UNIQUE (local_root_path_prefix, local_root_path),
	CONSTRAINT configuration_configurationfriendlyname_ck CHECK ((char_length(btrim((configuration_friendly_name)::text)) > 0)),
	CONSTRAINT configuration_localrootpaths_ck CHECK (((char_length((local_root_path_prefix)::text) > 0) OR (char_length((local_root_path)::text) > 0))),
	CONSTRAINT configuration_pollfrequencyseconds_ck CHECK ((poll_frequency_seconds >= 0))
);

CREATE TABLE configuration_kvp (
	configuration_id character varying(100) NOT NULL,
	key character varying(100) NOT NULL,
	value character varying(1000) NOT NULL,

	CONSTRAINT configurationkvp_configurationid_key_pk PRIMARY KEY (configuration_id, key),
	CONSTRAINT configurationkvp_key_ck CHECK ((char_length(btrim((key)::text)) > 0)),
	CONSTRAINT configurationkvp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE configuration_pgp (
	configuration_id character varying(100) NOT NULL,
	file_extension_filter character varying(100) NOT NULL,
	sender_public_key character varying NOT NULL,
	recipient_public_key character varying NOT NULL,
	recipient_private_key character varying NOT NULL,
	recipient_private_key_password character varying(1000) NOT NULL,

	CONSTRAINT configurationpgp_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configurationpgp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE configuration_sftp (
	configuration_id character varying(100) NOT NULL,
	hostname character varying(100) NOT NULL,
	port integer NOT NULL,
	remote_path character varying(1000) NOT NULL,
	username character varying(100) NOT NULL,
	client_public_key character varying NOT NULL,
	client_private_key character varying NOT NULL,
	client_private_key_password character varying(1000) NOT NULL,
	host_public_key character varying NOT NULL,

	CONSTRAINT configurationsftp_configurationid_pk PRIMARY KEY (configuration_id),
	CONSTRAINT configurationsftp_port_ck CHECK ((port > 0)),
	CONSTRAINT configurationsftp_remotepath_ck CHECK ((char_length(btrim((remote_path)::text)) > 0)),
	CONSTRAINT configurationsftp_username_ck CHECK ((char_length(btrim((username)::text)) > 0)),
	CONSTRAINT configurationsftp_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE eds_endpoint (
	single_row_lock boolean NOT NULL,
	eds_url character varying(1000) NOT NULL,
	software_content_type character varying(100) NOT NULL,
	software_version character varying(100) NOT NULL,
	use_keycloak boolean NOT NULL,
	keycloak_token_uri character varying(500),
	keycloak_realm character varying(100),
	keycloak_username character varying(100),
	keycloak_password character varying(100),
	keycloak_clientid character varying(100),

	CONSTRAINT edsendpoint_singlerowlock_pk PRIMARY KEY (single_row_lock),
	CONSTRAINT edsendpoint_edsurl_ck CHECK ((char_length(btrim((eds_url)::text)) > 0)),
	CONSTRAINT edsendpoint_keycloakclientid_ck CHECK (((keycloak_clientid IS NULL) OR (char_length(btrim((keycloak_clientid)::text)) > 0))),
	CONSTRAINT edsendpoint_keycloakfields_ck CHECK (((NOT use_keycloak) OR ((keycloak_token_uri IS NOT NULL) AND (keycloak_realm IS NOT NULL) AND (keycloak_username IS NOT NULL) AND (keycloak_password IS NOT NULL) AND (keycloak_clientid IS NOT NULL)))),
	CONSTRAINT edsendpoint_keycloakpassword_ck CHECK (((keycloak_password IS NULL) OR (char_length(btrim((keycloak_password)::text)) > 0))),
	CONSTRAINT edsendpoint_keycloakrealm_ck CHECK (((keycloak_realm IS NULL) OR (char_length(btrim((keycloak_realm)::text)) > 0))),
	CONSTRAINT edsendpoint_keycloaktokenuri_ck CHECK (((keycloak_token_uri IS NULL) OR (char_length(btrim((keycloak_token_uri)::text)) > 0))),
	CONSTRAINT edsendpoint_keycloakusername_ck CHECK (((keycloak_username IS NULL) OR (char_length(btrim((keycloak_username)::text)) > 0))),
	CONSTRAINT edsendpoint_singlerowlock_ck CHECK ((single_row_lock = true)),
	CONSTRAINT edsendpoint_softwarecontenttype_ck CHECK ((char_length(btrim((software_content_type)::text)) > 0)),
	CONSTRAINT edsendpoint_softwareversion_ck CHECK ((char_length(btrim((software_version)::text)) > 0))
);

CREATE TABLE emis_organisation_map (
	guid character varying NOT NULL,
	name character varying NOT NULL,
	ods_code character varying NOT NULL,

	CONSTRAINT emisorganisationmap_guid_pk PRIMARY KEY (guid)
);

CREATE INDEX configuration_emisorganisationmap_guid_ix ON emis_organisation_map USING btree (guid);

CREATE TABLE instance (
	instance_name character varying(100) NOT NULL,
	hostname character varying(500),
	http_management_port integer,
	last_config_get_date timestamp without time zone,

	CONSTRAINT instance_instancename_pk PRIMARY KEY (instance_name),
	CONSTRAINT instance_hostname_ck CHECK (((hostname IS NULL) OR (char_length(btrim((hostname)::text)) > 0))),
	CONSTRAINT instance_httpmanagementport_ck CHECK (((http_management_port IS NULL) OR (http_management_port > 0))),
	CONSTRAINT instance_instancename_ck CHECK ((char_length(btrim((instance_name)::text)) > 0))
);

CREATE TABLE instance_configuration (
	instance_name character varying(100) NOT NULL,
	configuration_id character varying(100) NOT NULL,

	CONSTRAINT instanceconfiguration_instancename_configid_pk PRIMARY KEY (instance_name, configuration_id),
	CONSTRAINT instanceconfiguration_configurationid_uq UNIQUE (configuration_id),
	CONSTRAINT instanceconfiguration_instancename_fk FOREIGN KEY (instance_name) REFERENCES instance (instance_name),
	CONSTRAINT instanceconfiguration_configurationid_fk FOREIGN KEY (configuration_id) REFERENCES configuration (configuration_id)
);

CREATE TABLE slack_endpoint (
	single_row_lock boolean NOT NULL,
	enabled boolean NOT NULL,
	slack_url character varying(1000) NOT NULL,

	CONSTRAINT slackendpoint_singlerowlock_pk PRIMARY KEY (single_row_lock),
	CONSTRAINT slackendpoint_enabled_slackurl_ck CHECK (((enabled AND (char_length(btrim((slack_url)::text)) > 0)) OR (NOT enabled))),
	CONSTRAINT slackendpoint_singlerowlock_ck CHECK (single_row_lock)
);

CREATE TABLE batch (
	batch_id integer NOT NULL,
	configuration_id character varying(100) NOT NULL,
	interface_type_id integer NOT NULL,
	batch_identifier character varying(500) NOT NULL,
	local_relative_path character varying(1000) NOT NULL,
	insert_date timestamp without time zone DEFAULT date_trunc('second'::text, (now())::timestamp without time zone) NOT NULL,
	sequence_number integer,
	is_complete boolean DEFAULT false NOT NULL,
	complete_date timestamp without time zone,

	CONSTRAINT batch_filesetid_pk PRIMARY KEY (batch_id),
	CONSTRAINT batch_configurationid_batchid_uq UNIQUE (configuration_id, batch_id),
	CONSTRAINT batch_configurationid_batchidentifier_uq UNIQUE (configuration_id, batch_identifier),
	CONSTRAINT batch_configurationid_sequencenumber_uq UNIQUE (configuration_id, sequence_number),
	CONSTRAINT batch_batchidentifier_ck CHECK ((char_length(btrim((batch_identifier)::text)) > 0)),
	CONSTRAINT batch_insertdate_completedate_ck CHECK (((complete_date IS NULL) OR (insert_date <= complete_date))),
	CONSTRAINT batch_iscomplete_completedate_sequencenumber_ck CHECK (((is_complete AND (complete_date IS NOT NULL) AND (sequence_number IS NOT NULL)) OR ((NOT is_complete) AND (complete_date IS NULL) AND (sequence_number IS NULL)))),
	CONSTRAINT batch_sequencenumber_ck CHECK (((sequence_number IS NULL) OR (sequence_number > 0))),
	CONSTRAINT batch_configurationid_interfacetypeid_fk FOREIGN KEY (configuration_id, interface_type_id) REFERENCES configuration (configuration_id, interface_type_id)
);

CREATE TABLE batch_file (
	batch_file_id integer NOT NULL,
	batch_id integer NOT NULL,
	interface_type_id integer NOT NULL,
	file_type_identifier character varying(1000) NOT NULL,
	insert_date timestamp without time zone DEFAULT date_trunc('second'::text, (now())::timestamp without time zone) NOT NULL,
	filename character varying(1000) NOT NULL,
	remote_created_date timestamp without time zone NOT NULL,
	remote_size_bytes bigint NOT NULL,
	is_downloaded boolean DEFAULT false NOT NULL,
	download_date timestamp without time zone,
	local_size_bytes bigint,
	requires_decryption boolean NOT NULL,
	is_decrypted boolean,
	decrypt_date timestamp without time zone,
	decrypted_filename character varying(1000),
	decrypted_size_bytes bigint,

	CONSTRAINT batchfile_batchfileid_pk PRIMARY KEY (batch_file_id),
	CONSTRAINT batchfile_batchid_decryptedfilename_uq UNIQUE (batch_id, decrypted_filename),
	CONSTRAINT batchfile_batchid_filename_uq UNIQUE (batch_id, filename),
	CONSTRAINT batchfile_batchid_filetypeidentifier_uq UNIQUE (batch_id, file_type_identifier),
	CONSTRAINT batchfile_decryptedcolumns_ck CHECK ((((is_decrypted IS NOT NULL) AND is_decrypted AND (decrypt_date IS NOT NULL) AND (decrypted_filename IS NOT NULL) AND (decrypted_size_bytes IS NOT NULL)) OR (((is_decrypted IS NULL) OR (NOT is_decrypted)) AND (decrypt_date IS NULL) AND (decrypted_filename IS NULL) AND (decrypted_size_bytes IS NULL)))),
	CONSTRAINT batchfile_decryptedfilename_ck CHECK (((decrypted_filename IS NULL) OR (char_length(btrim((decrypted_filename)::text)) > 0))),
	CONSTRAINT batchfile_decryptedsizebytes_ck CHECK ((decrypted_size_bytes >= 0)),
	CONSTRAINT batchfile_downloaddate_decryptdate_ck CHECK (((download_date IS NULL) OR (decrypt_date IS NULL) OR (download_date <= decrypt_date))),
	CONSTRAINT batchfile_filename_ck CHECK ((char_length(btrim((filename)::text)) > 0)),
	CONSTRAINT batchfile_insertdate_downloaddate_ck CHECK (((download_date IS NULL) OR (insert_date <= download_date))),
	CONSTRAINT batchfile_isdownloaded_downloaddate_localfilesizebytes_ck CHECK (((is_downloaded AND (download_date IS NOT NULL) AND (local_size_bytes IS NOT NULL)) OR ((NOT is_downloaded) AND (download_date IS NULL) AND (local_size_bytes IS NULL)))),
	CONSTRAINT batchfile_isdownloaded_isdecrypted_ck CHECK (((NOT is_decrypted) OR is_downloaded)),
	CONSTRAINT batchfile_localsizebytes_ck CHECK ((local_size_bytes >= 0)),
	CONSTRAINT batchfile_remotesizebytes_ck CHECK ((remote_size_bytes >= 0)),
	CONSTRAINT batchfile_requiresdecryption_isdecrypted_ck CHECK ((((NOT requires_decryption) AND (is_decrypted IS NULL)) OR (requires_decryption AND (is_decrypted IS NOT NULL)))),
	CONSTRAINT batchfile_batchid_fk FOREIGN KEY (batch_id) REFERENCES batch(batch_id),
	CONSTRAINT batchfile_interfacetypeid_filetypeidentifier_fk FOREIGN KEY (interface_type_id, file_type_identifier) REFERENCES interface_file_type (interface_type_id, file_type_identifier)
);

CREATE TABLE batch_split (
	batch_split_id integer NOT NULL,
	batch_id integer NOT NULL,
	configuration_id character varying(100) NOT NULL,
	local_relative_path character varying(1000) NOT NULL,
	organisation_id character varying(100) NOT NULL,
	have_notified boolean DEFAULT false NOT NULL,
	notification_date timestamp without time zone,

	CONSTRAINT batchsplit_batchsplitid_pk PRIMARY KEY (batch_split_id),
	CONSTRAINT batchsplit_configurationid_batchid_batchsplitid_uq UNIQUE (configuration_id, batch_id, batch_split_id),
	CONSTRAINT batchsplit_configurationid_batchid_fk FOREIGN KEY (configuration_id, batch_id) REFERENCES batch (configuration_id, batch_id)
);

CREATE TABLE error_digest (
	error_digest_id integer NOT NULL,
	error_count integer NOT NULL,
	last_log_date timestamp without time zone NOT NULL,
	log_class character varying(1000) NOT NULL,
	log_method character varying(1000) NOT NULL,
	log_message character varying(1000) NOT NULL,
	exception character varying NOT NULL,

	CONSTRAINT errordigest_errorcount_ck CHECK ((error_count > 0)),
	CONSTRAINT errordigest_errordigestid_pk PRIMARY KEY (error_digest_id),
	CONSTRAINT errordigest_logclass_logmethod_logmessage_exception_uq UNIQUE (log_class, log_method, log_message, exception)
);

CREATE TABLE notification_message (
	notification_message_id integer NOT NULL,
	batch_id integer NOT NULL,
	batch_split_id integer NOT NULL,
	configuration_id character varying(100) NOT NULL,
	message_uuid uuid NOT NULL,
	"timestamp" timestamp without time zone NOT NULL,
	outbound character varying NOT NULL,
	inbound character varying,
	was_success boolean NOT NULL,
	error_text character varying,

	CONSTRAINT notificationmessage_notificationmessageid_pk PRIMARY KEY (notification_message_id),
	CONSTRAINT notificationmessage_messageuuid_uq UNIQUE (message_uuid),
	CONSTRAINT notificationmessage_inbound_wassuccess_ck CHECK (((inbound IS NOT NULL) OR (NOT was_success))),
	CONSTRAINT notificationmessage_outbound_ck CHECK ((char_length(btrim((outbound)::text)) > 0)),
	CONSTRAINT notificationmessage_wassuccess_errortext_ck CHECK (((was_success AND (error_text IS NULL)) OR ((NOT was_success) AND (error_text IS NOT NULL)))),
	CONSTRAINT notificationmessage_configurationid_batchid_batchsplitid_fk FOREIGN KEY (configuration_id, batch_id, batch_split_id) REFERENCES batch_split (configuration_id, batch_id, batch_split_id)
);

CREATE TABLE unknown_file (
	unknown_file_id integer NOT NULL,
	configuration_id character varying(100) NOT NULL,
	insert_date timestamp without time zone DEFAULT date_trunc('second'::text, (now())::timestamp without time zone) NOT NULL,
	filename character varying(1000) NOT NULL,
	remote_created_date timestamp without time zone NOT NULL,
	remote_size_bytes bigint NOT NULL,

	CONSTRAINT unknownfile_unknownfileid_pk PRIMARY KEY (unknown_file_id),
	CONSTRAINT unknownfile_filename_ck CHECK ((char_length(btrim((filename)::text)) > 0)),
	CONSTRAINT unknownfile_remotesizebytes_ck CHECK ((remote_size_bytes >= 0)),
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

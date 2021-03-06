/*
	Schema V1.5: Rename instance_id columns to configuration_id
*/

-- 1a. drop constraint configuration_configuration_instanceid_pk and dependents
alter table log.unknown_file drop constraint log_unknownfile_instanceid_fk;
alter table configuration.configuration_sftp drop constraint configuration_configurationsftp_instanceid_fk;
alter table configuration.configuration_sftp drop constraint configuration_configurationsftp_instanceid_pk;
alter table configuration.configuration_pgp drop constraint configuration_configurationpgp_instanceid_fk;
alter table configuration.configuration_pgp drop constraint configuration_configurationpgp_intsanceid_pk;
alter table configuration.configuration_kvp drop constraint configuration_configuration_instanceid_key_pk;
alter table configuration.configuration_kvp drop constraint configuration_configuration_instanceid_fk;
alter table configuration.configuration drop constraint configuration_configuration_instanceid_pk;

-- 1b. drop constraint configuration_configuration_instanceid_interfacetypeid_uq and dependents
alter table log.batch drop constraint log_batch_instanceid_interfacetypeid_fk;
alter table configuration.configuration drop constraint configuration_configuration_instanceid_interfacetypeid_uq;

-- 1c. drop constraint log_batch_instanceid_batchid_uq and dependents
alter table log.batch_split drop constraint log_batchsplit_instanceid_interfacetypeid_fk;
alter table log.batch drop constraint log_batch_instanceid_batchid_uq;

-- 1d. drop constraint log_batchsplit_instanceid_batchid_uq and dependents
alter table log.notification_message drop constraint log_notificationmessage_batchid_instanceid_fk;
alter table log.batch_split drop constraint log_batchsplit_instanceid_batchid_uq;

-- 1e. drop constraint log_batch_instanceid_batchidentifier_uq
alter table log.batch drop constraint log_batch_instanceid_batchidentifier_uq;

-- 1f. drop constraint log_batch_instanceid_sequencenumber_uq
alter table log.batch drop constraint log_batch_instanceid_sequencenumber_uq;

-- 2. rename instance_id columns to configuration_id
alter table configuration.configuration rename instance_id to configuration_id;
alter table configuration.configuration rename local_instance_path_prefix to local_root_path_prefix;
alter table configuration.configuration rename local_instance_path to local_root_path;
alter table configuration.configuration rename instance_friendly_name to configuration_friendly_name;
alter table configuration.configuration_kvp rename instance_id to configuration_id;
alter table configuration.configuration_pgp rename instance_id to configuration_id;
alter table configuration.configuration_sftp rename instance_id to configuration_id;
alter table log.batch rename instance_id to configuration_id;
alter table log.batch_split rename instance_id to configuration_id;
alter table log.notification_message rename instance_id to configuration_id;
alter table log.unknown_file rename instance_id to configuration_id;

-- 3a. recreate constraint configuration_configuration_configurationid_pk and dependents
alter table configuration.configuration add constraint configuration_configuration_configurationid_pk primary key (configuration_id);
alter table configuration.configuration_sftp add constraint configuration_configurationsftp_configurationid_pk primary key (configuration_id);
alter table configuration.configuration_sftp add constraint configuration_configurationsftp_configurationid_fk foreign key (configuration_id) references configuration.configuration(configuration_id);
alter table configuration.configuration_pgp add constraint configuration_configurationpgp_configurationid_pk primary key (configuration_id);
alter table configuration.configuration_pgp add constraint configuration_configurationpgp_configurationid_fk foreign key (configuration_id) references configuration.configuration(configuration_id);
alter table configuration.configuration_kvp add constraint configuration_configuration_configurationid_key_pk primary key (configuration_id, key);
alter table configuration.configuration_kvp add constraint configuration_configurationkvp_configurationid_fk foreign key (configuration_id) references configuration.configuration(configuration_id);
alter table log.unknown_file add constraint log_unknownfile_configurationid_fk foreign key (configuration_id) references configuration.configuration(configuration_id);

-- 3b. recreate constraint configuration_configuration_configurationid_interfacetypeid_uq and dependents
alter table configuration.configuration add constraint configuration_configuration_configurationid_interfacetypeid_uq unique (configuration_id, interface_type_id);
alter table log.batch add constraint log_batch_configurationid_interfacetypeid_fk foreign key (configuration_id, interface_type_id) references configuration.configuration (configuration_id, interface_type_id);

-- 3c. recreate constraint log_batch_configurationid_batchid_uq and dependents
alter table log.batch add constraint log_batch_configurationid_batchid_uq unique (configuration_id, batch_id);
alter table log.batch_split add constraint log_batchsplit_configurationid_batchid_fk foreign key (configuration_id, batch_id) references log.batch (configuration_id, batch_id);

-- 3d. recreate constraint log_batchsplit_configurationid_batchid_batchsplitid_uq and dependents
alter table log.batch_split add constraint log_batchsplit_configurationid_batchid_batchsplitid_uq unique (configuration_id, batch_id, batch_split_id);
alter table log.notification_message add constraint log_notificationmessage_configurationid_batchid_batchsplitid_fk foreign key (configuration_id, batch_id, batch_split_id) references log.batch_split(configuration_id, batch_id, batch_split_id);

-- 3e. recreate constraint log_batch_configurationid_batchidentifier_uq
alter table log.batch add constraint log_batch_configurationid_batchidentifier_uq unique (configuration_id, batch_identifier);

-- 3f. recreate constraint log_batch_configurationid_sequencenumber_uq
alter table log.batch add constraint log_batch_configurationid_sequencenumber_uq unique (configuration_id, sequence_number);

-- 4. rename remaining constraints
alter table configuration.configuration rename constraint configuration_configuration_localinstancepaths_uq to configuration_configuration_localrootpaths_uq;
alter table configuration.configuration rename constraint configuration_configuration_localinstancepaths_ck to configuration_configuration_localrootpaths_ck;
alter table configuration.configuration rename constraint configuration_configuration_instancefriendlyname_uq to configuration_configuration_configurationfriendlyname_uq;
alter table configuration.configuration rename constraint configuration_configuration_instancefriendlyname_ck to configuration_configuration_configurationfriendlyname_ck;

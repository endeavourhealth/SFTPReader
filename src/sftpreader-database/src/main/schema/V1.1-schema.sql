/* 
	Schema V1.1: split configuration paths into two to allow sending of partial (relative) path to onward destination
*/

alter table configuration.configuration add local_instance_path varchar(1000) null;

update configuration.configuration set local_instance_path = '';

alter table configuration.configuration alter column local_instance_path set not null;

alter table configuration.configuration drop constraint configuration_configuration_localrootpath_uq;
alter table configuration.configuration drop constraint configuration_configuration_localrootpath_ck;

alter table configuration.configuration rename local_root_path to local_instance_path_prefix;

alter table configuration.configuration add constraint configuration_configuration_localinstancepaths_uq unique (local_instance_path_prefix, local_instance_path);
alter table configuration.configuration add constraint configuration_configuration_localinstancepaths_ck check ((char_length(local_instance_path_prefix) > 0) or (char_length(local_instance_path) > 0));

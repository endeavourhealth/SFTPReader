alter table configuration.configuration add local_instance_path varchar(1000) null;

update configuration.configuration set local_instance_path = '';

alter table configuration.configuration alter column local_instance_path set not null;

alter table configuration.configuration drop constraint configuration_configuration_localrootpath_uq;
alter table configuration.configuration drop constraint configuration_configuration_localrootpath_ck;

alter table configuration.configuration add constraint configuration_configuration_localrootpath_localinstancepath_uq unique (local_root_path, local_instance_path);
alter table configuration.configuration add constraint configuration_configuration_localrootpath_localinstancepath_ck check ((char_length(local_root_path) > 0) or (char_length(local_instance_path) > 0));

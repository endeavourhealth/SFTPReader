/*
	Schema V1.6: Add instance tables
*/

create table configuration.instance
(
	instance_name varchar(100) not null,
	hostname varchar(500) null,
	last_config_get_date timestamp null,
	
	constraint configuration_instance_instancename_pk primary key (instance_name),
	constraint configuration_instance_instancename_ck check (char_length(trim(instance_name)) > 0),
	constraint configuration_instance_hostname_ck check (hostname is null or char_length(trim(hostname)) > 0)
);

create table configuration.instance_configuration
(
	instance_name varchar(100) not null,
	configuration_id varchar(100) not null,
	
	constraint configuration_instanceconfiguration_instancename_configurationid_pk primary key (instance_name, configuration_id),
	constraint configuration_instanceconfiguration_instancename_fk foreign key (instance_name) references configuration.instance (instance_name),
	constraint configuration_instanceconfiguration_configurationid_fk foreign key (configuration_id) references configuration.configuration (configuration_id),
	constraint configuration_instanceconfiguration_configurationid_uq unique (configuration_id)
);

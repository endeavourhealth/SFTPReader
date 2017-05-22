/*
	Schema V1.7: Add management http port to configuration.instance
*/

alter table configuration.instance add http_management_port integer null;
alter table configuration.instance add constraint configuration_instance_httpmanagementport_ck check (http_management_port is null or (http_management_port > 0));

alter table configuration.instance drop last_config_get_date;
alter table configuration.instance add last_config_get_date timestamp null;

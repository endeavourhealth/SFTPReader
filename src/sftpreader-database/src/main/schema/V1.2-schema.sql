/* 
	Schema V1.2: Have only one slack configuration rather than one per configuration, rename description column to instance_friendly_name
*/

create table configuration.slack
(
	single_row_lock boolean not null,
	enabled boolean not null,
	slack_url varchar(1000) not null,
	
	constraint configuration_slack_singlerowlock_pk primary key (single_row_lock),
	constraint configuration_slack_singlerowlock_ck check (single_row_lock),
	constraint configuration_slack_enabled_slackurl_ck check ((enabled and char_length(trim(slack_url)) > 0) or not enabled)
);

drop table configuration.configuration_slack;

alter table configuration.instance rename description to instance_friendly_name;

alter table configuration.instance drop constraint configuration_instance_description_uq;
alter table configuration.instance drop constraint configuration_instance_description_ck;

alter table configuration.instance add constraint configuration_instance_instancefriendlyname_uq unique (instance_friendly_name);
alter table configuration.instance add constraint configuration_instance_instancefriendlyname_ck check (char_length(trim(instance_friendly_name)) > 0);
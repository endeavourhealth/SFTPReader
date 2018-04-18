/* 
	Schema V1.4: drop configuration.instance table
*/

alter table configuration.configuration add column instance_friendly_name varchar(100) null;

update configuration.configuration c
set instance_friendly_name = i.instance_friendly_name
from configuration.instance i 
where c.instance_id = i.instance_id;

alter table configuration.configuration alter column instance_friendly_name set not null;

alter table configuration.configuration add constraint configuration_configuration_instancefriendlyname_uq unique (instance_friendly_name);
alter table configuration.configuration add constraint configuration_configuration_instancefriendlyname_ck check (char_length(trim(instance_friendly_name)) > 0);

alter table configuration.configuration drop constraint configuration_configuration_instanceid_fk;

drop table configuration.instance;
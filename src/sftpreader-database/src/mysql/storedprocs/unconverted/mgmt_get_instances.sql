
create or replace function management.get_instances
()
returns table
(
	instance_name varchar,
	hostname varchar,
	http_management_port integer,
	last_config_get_date timestamp
)
as $$

	select
		i.instance_name,
		i.hostname,
		i.http_management_port,
		i.last_config_get_date
	from configuration.instance i;
	
$$ language sql;

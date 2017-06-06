
create or replace function configuration.get_instance_configuration
(
	_instance_name varchar(100),
	_hostname varchar(500)
)
returns setof refcursor
as $$
declare
	_existing_hostname varchar(500);
	instance refcursor;
	configuration_instance refcursor;
	configuration_eds refcursor;
	configuration_slack refcursor;
begin

	------------------------------------------------------
	-- perform instance checks
	------------------------------------------------------
	if not exists 
	(
		select * 
		from configuration.instance i
		where i.instance_name = _instance_name
	)
	then
		raise exception 'INSTANCE_NAME % not found', _instance_name;
		return;
	end if;

	select 
		i.hostname into _existing_hostname
	from configuration.instance i
	where i.instance_name = _instance_name;
	
	if (_existing_hostname is null)
	then
		update configuration.instance
		set hostname = _hostname
		where instance_name = _instance_name;
	end if;
	
	if not exists
	(
		select *
		from configuration.instance i
		where i.instance_name = _instance_name
		and i.hostname = _hostname
	)
	then
		raise exception 'Hostname for INSTANCE_NAME % has changed, if this is intentional please update the configuration to reflect the changes', _instance_name;
		return;
	end if;
	
	if not exists
	(
		select *
		from configuration.instance_configuration ic		where ic.instance_name = _instance_name
	)
	then
		raise exception 'There are no configurations associated with INSTANCE_NAME %', _instance_name;
		return;
	end if;
	
	update configuration.instance
	set last_config_get_date = now()
	where instance_name = _instance_name;

	------------------------------------------------------
	-- now get the configuration
	------------------------------------------------------
	instance = 'instance';
	
	open instance for
	select
		instance_name,
		http_management_port
	from configuration.instance
	where instance_name = _instance_name;
	
	return next instance;
	
	------------------------------------------------------
	configuration_instance = 'configuration_instance';
	
	open configuration_instance for
	select
		configuration_id
	from configuration.instance_configuration	where instance_name = _instance_name;
	
	return next configuration_instance;
	
	------------------------------------------------------
	configuration_eds = 'configuration_eds';
	
	open configuration_eds for
	select
		e.eds_url,
		e.software_content_type,
		e.software_version,
		e.use_keycloak,
		e.keycloak_token_uri,
		e.keycloak_realm,
		e.keycloak_username,
		e.keycloak_password,
		e.keycloak_clientid
	from configuration.eds e;
	
	return next configuration_eds;
	
	------------------------------------------------------
	configuration_slack = 'configuration_slack';
	
	open configuration_slack for
	select
		coalesce(s.enabled, false) as slack_enabled,
		s.slack_url
	from (select 1) a
	left outer join configuration.slack s on single_row_lock = true;
	
	return next configuration_slack;
	
	------------------------------------------------------
		
end;
$$ language plpgsql;

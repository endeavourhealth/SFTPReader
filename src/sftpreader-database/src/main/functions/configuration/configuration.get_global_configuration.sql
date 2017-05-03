
create or replace function configuration.get_global_configuration
(
	_hostname varchar(100)
)
returns setof refcursor
as $$
declare
	configuration_eds refcursor;
	configuration_slack refcursor;
begin

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

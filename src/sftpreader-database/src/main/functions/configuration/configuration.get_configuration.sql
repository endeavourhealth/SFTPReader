
create or replace function configuration.get_configuration
(
	_configuration_id varchar(100)
)
returns setof refcursor
as $$
declare
	configuration refcursor;
	configuration_sftp refcursor;
	configuration_pgp refcursor;
	configuration_kvp refcursor;
	configuration_filetypeidentifier refcursor;
begin

	------------------------------------------------------
	configuration = 'configuration';
	
	open configuration for
	select
		c.configuration_id,
		c.configuration_friendly_name,
		it.interface_type_name,
		c.poll_frequency_seconds,	
		c.local_root_path_prefix,
		c.local_root_path,
		c.software_content_type,
		c.software_version
	from configuration.configuration c 
	inner join configuration.interface_type it on c.interface_type_id = it.interface_type_id
	where c.configuration_id = _configuration_id;

	return next configuration;
	
	------------------------------------------------------
	configuration_sftp = 'configuration_sftp';
	
	open configuration_sftp for	
	select
		cs.hostname,
		cs.port,
		cs.remote_path,
		cs.username,
		cs.client_private_key,
		cs.client_private_key_password,
		cs.host_public_key
	from configuration.configuration_sftp cs
	where cs.configuration_id = _configuration_id;
	
	return next configuration_sftp;
	
	------------------------------------------------------
	configuration_pgp = 'configuration_pgp';
	
	open configuration_pgp for	
	select
		cp.file_extension_filter as pgp_file_extension_filter,
		cp.sender_public_key as pgp_sender_public_key,
		cp.recipient_public_key as pgp_recipient_public_key,
		cp.recipient_private_key as pgp_recipient_private_key,
		cp.recipient_private_key_password as pgp_recipient_private_key_password
	from configuration.configuration_pgp cp
	where cp.configuration_id = _configuration_id;
	
	return next configuration_pgp;
	
	------------------------------------------------------
	configuration_kvp = 'configuration_kvp';
	
	open configuration_kvp for
	select
		kvp.key,
		kvp.value
	from configuration.configuration_kvp kvp
	where kvp.configuration_id = _configuration_id;
	
	return next configuration_kvp;
	
	------------------------------------------------------
	configuration_filetypeidentifier = 'configuration_filetypeidentifier';
	
	open configuration_filetypeidentifier for
	select
		ift.file_type_identifier
	from configuration.configuration c
	inner join configuration.interface_type it on c.interface_type_id = it.interface_type_id
	inner join configuration.interface_file_type ift on ift.interface_type_id = it.interface_type_id
	where c.configuration_id = _configuration_id;
	
	return next configuration_filetypeidentifier;
	
	------------------------------------------------------

end;
$$ language plpgsql;

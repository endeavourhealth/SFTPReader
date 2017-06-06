delimiter $$

drop procedure if exists get_configuration;

$$

create procedure get_configuration
(
	configuration_id varchar(100)
)
begin

	select
		c.configuration_id,
		c.configuration_friendly_name,
		it.interface_type_name,
		c.poll_frequency_seconds,	
		c.local_root_path_prefix,
		c.local_root_path
	from configuration c 
	inner join interface_type it on c.interface_type_id = it.interface_type_id
	where c.configuration_id = configuration_id;

	select
		cs.hostname,
		cs.port,
		cs.remote_path,
		cs.username,
		cs.client_private_key,
		cs.client_private_key_password,
		cs.host_public_key
	from configuration_sftp cs
	where cs.configuration_id = configuration_id;

	select
		cp.file_extension_filter as pgp_file_extension_filter,
		cp.sender_public_key as pgp_sender_public_key,
		cp.recipient_public_key as pgp_recipient_public_key,
		cp.recipient_private_key as pgp_recipient_private_key,
		cp.recipient_private_key_password as pgp_recipient_private_key_password
	from configuration_pgp cp
	where cp.configuration_id = configuration_id;

	select
		kvp.configuration_key,
		kvp.configuration_value
	from configuration_kvp kvp
	where kvp.configuration_id = configuration_id;
	
	select
		ift.file_type_identifier
	from configuration c
	inner join interface_type it on c.interface_type_id = it.interface_type_id
	inner join interface_file_type ift on ift.interface_type_id = it.interface_type_id
	where c.configuration_id = configuration_id;

end

$$

delimiter ;


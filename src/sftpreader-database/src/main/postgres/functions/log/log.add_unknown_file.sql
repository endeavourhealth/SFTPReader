
create or replace function log.add_unknown_file
(
	_configuration_id varchar,
	_filename varchar,
	_remote_size_bytes bigint,
	_remote_created_date timestamp
)
returns void
as $$
begin

	if not exists
	(
		select *
		from log.unknown_file
		where configuration_id = _configuration_id
		and filename = _filename
	)
	then
		insert into log.unknown_file
		(
			configuration_id,
			filename,
			remote_created_date,
			remote_size_bytes
		)
		values
		(
			_configuration_id,
			_filename,
			_remote_created_date,
			_remote_size_bytes
		);
	end if;

end
$$ language plpgsql;


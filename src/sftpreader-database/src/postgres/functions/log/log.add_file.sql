
create or replace function log.add_file
	(
		_configuration_id varchar,
		_batch_identifier varchar,
		_file_type_identifier varchar,
		_filename varchar,
		_local_relative_path varchar,
		_remote_size_bytes bigint,
		_remote_created_date timestamp
	)
	returns table
	(
	file_already_processed boolean,
	batch_file_id integer
	)
as $$
declare _batch_id integer;
				declare _interface_type_id integer;
				declare _batch_file_id integer;
				declare _file_already_processed boolean;
begin

	select
		batch_id
	into
		_batch_id
	from log.batch
	where configuration_id = _configuration_id
				and batch_identifier = _batch_identifier;

	select
		interface_type_id
	into
		_interface_type_id
	from configuration.configuration
	where configuration_id = _configuration_id;

	if (_batch_id is null)
	then

		insert into log.batch
		(
			configuration_id,
			batch_identifier,
			interface_type_id,
			local_relative_path
		)
		values
			(
				_configuration_id,
				_batch_identifier,
				_interface_type_id,
				_local_relative_path
			)
		returning batch_id into _batch_id;

	end if;

	select
		bf.batch_file_id,
		is_downloaded
	into
		_batch_file_id,
		_file_already_processed
	from log.batch_file bf
	where batch_id = _batch_id
				and file_type_identifier = _file_type_identifier
				and filename = _filename;

	if (_batch_file_id is not null)
	then
		if (not _file_already_processed)
		then
			delete from log.batch_file bf
			where bf.batch_file_id = _batch_file_id;

			_batch_file_id = null;
		end if;
	end if;

	if (_batch_file_id is null)
	then
		insert into log.batch_file
		(
			batch_id,
			interface_type_id,
			file_type_identifier,
			filename,
			remote_size_bytes,
			remote_created_date
		)
		values
			(
				_batch_id,
				_interface_type_id,
				_file_type_identifier,
				_filename,
				_remote_size_bytes,
				_remote_created_date
			)
		returning log.batch_file.batch_file_id into _batch_file_id;
	end if;

	return query
	select
		_file_already_processed as file_already_processed,
		_batch_file_id as batch_file_id;

end;
$$ language plpgsql;


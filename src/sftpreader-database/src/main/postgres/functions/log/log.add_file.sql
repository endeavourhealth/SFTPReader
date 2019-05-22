
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
				declare _file_already_downloaded boolean;
begin


	-- find existing file if possible
	select
		f.batch_file_id,
		f.is_downloaded
	into
		_batch_file_id,
		_file_already_downloaded
	from log.batch b
		inner join log.batch_file f
			on b.batch_id = f.batch_id
	where
		configuration_id = _configuration_id
		and batch_identifier = _batch_identifier
		and file_type_identifier = _file_type_identifier
		and filename = _filename;

	-- delete file if not downloaded
	if (_batch_file_id is not null
			&& _file_already_processed is not null
			&& not _file_already_downloaded)
	then
		delete from log.batch_file bf
		where bf.batch_file_id = _batch_file_id;

		_batch_file_id = null;
	end if;

	-- if no batch file, try to find a suitable batch
	if (_batch_file_id is null)
	then

		-- look for a non-complete one
		select b.batch_id
		into
			_batch_id
		from log.batch b
		where
			configuration_id = _configuration_id
			and batch_identifier = _batch_identifier
			and is_complete = false;

		-- we'll need the interface type ID
		select interface_type_id
		into
			_interface_type_id
		from configuration.configuration
		where configuration_id = _configuration_id;

		-- if no non-complete batch, create one
		if (_batch_id is null)
		then
			insert into log.batch (
				configuration_id,
				batch_identifier,
				interface_type_id,
				local_relative_path
			) values (
				_configuration_id,
				_batch_identifier,
				_interface_type_id,
				_local_relative_path
			) returning batch_id into _batch_id;

		end if;

		-- batch file
		insert into log.batch_file (
			batch_id,
			interface_type_id,
			file_type_identifier,
			filename,
			remote_size_bytes,
			remote_created_date
		) values (
			_batch_id,
			_interface_type_id,
			_file_type_identifier,
			_filename,
			_remote_size_bytes,
			_remote_created_date
		) returning log.batch_file.batch_file_id into _batch_file_id;

		-- we know we've not downloaded it
		_file_already_downloaded = false;

	end if;

	return query
	select
		_file_already_processed as file_already_processed,
		_batch_file_id as batch_file_id;

end;
$$ language plpgsql;


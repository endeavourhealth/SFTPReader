alter table log.batch_split
add is_bulk boolean;

DROP FUNCTION log.add_batch_split(integer, character varying, character varying, character varying);


create or replace function log.add_batch_split
(
	_batch_id int,
	_configuration_id varchar,
	_local_relative_path varchar,
	_organisation_id varchar,
	_is_bulk boolean
)
returns void
as $$
begin

	insert into log.batch_split
	(
		batch_id,
		configuration_id,
		local_relative_path,
		organisation_id,
		is_bulk
	)
	values
	(
		_batch_id,
		_configuration_id,
		_local_relative_path,
		_organisation_id,
		_is_bulk
	);

end;
$$ language plpgsql;


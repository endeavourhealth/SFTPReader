
create or replace function log.add_batch_split
(
	_batch_id int,
	_configuration_id varchar,
	_local_relative_path varchar,
	_organisation_id varchar
)
returns void
as $$
begin

	insert into log.batch_split
	(
		batch_id,
		configuration_id,
		local_relative_path,
		organisation_id
	)
	values
	(
		_batch_id,
		_configuration_id,
		_local_relative_path,
		_organisation_id
	);

end;
$$ language plpgsql;


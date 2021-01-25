
create or replace function log.add_batch_split
(
	_batch_id int,
	_configuration_id varchar,
	_local_relative_path varchar,
	_organisation_id varchar,
	_is_bulk boolean,
	_has_patient_data boolean
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
		is_bulk,
		has_patient_data
	)
	values
	(
		_batch_id,
		_configuration_id,
		_local_relative_path,
		_organisation_id,
		_is_bulk,
		_has_patient_data
	);

end;
$$ language plpgsql;


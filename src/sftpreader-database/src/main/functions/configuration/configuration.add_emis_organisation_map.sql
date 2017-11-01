
-- have to explicitly drop the old function
DROP FUNCTION IF EXISTS configuration.add_emis_organisation_map(character varying, character varying, character varying);

create or replace function configuration.add_emis_organisation_map
(
    _guid varchar,
    _name varchar,
    _ods_code varchar,
    _start_date date
)
returns void as
$$

	delete
	from configuration.emis_organisation_map
	where guid = _guid;

	insert into configuration.emis_organisation_map
	(
		guid,
		name,
		ods_code,
		start_date
	)
	values
	(
		_guid,
		_name,
		_ods_code,
		_start_date
	);

$$ language sql;

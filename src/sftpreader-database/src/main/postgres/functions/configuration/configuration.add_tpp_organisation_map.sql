
-- have to explicitly drop the old function
DROP FUNCTION IF EXISTS configuration.add_tpp_organisation_map(character varying, character varying);

create or replace function configuration.add_tpp_organisation_map
(
    _ods_code varchar,
    _name varchar
)
returns void as
$$

	delete
	from configuration.tpp_organisation_map
	where ods_code = _ods_code;

	insert into configuration.tpp_organisation_map
	(
		ods_code,
		name
	)
	values
	(
		_ods_code,
		_name
	);

$$ language sql;

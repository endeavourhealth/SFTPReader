
-- have to explicitly drop the old function
DROP FUNCTION IF EXISTS configuration.get_emis_organisation_map(character varying);

create or replace function configuration.get_emis_organisation_map
(
	_guid varchar
)
returns table
(
	guid varchar,
	name varchar,
	ods_code varchar
)
as $$

	select
		guid,
		name,
		ods_code
	from configuration.emis_organisation_map
	where guid = _guid;

$$ language sql;

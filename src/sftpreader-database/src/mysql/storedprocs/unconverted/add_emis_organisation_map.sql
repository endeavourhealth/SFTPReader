delimiter $$

drop procedure if exists add_emis_organisation_map;

$$

create procedure add_emis_organisation_map
(
    guid varchar(100),
    name varchar(1000),
    ods_code varchar(100)
)
begin
	delete e
	from emis_organisation_map e
	where e.guid = guid;

	insert into emis_organisation_map
	(
		guid,
		name,
		ods_code
	)
	values
	(
		guid,
		name,
		ods_code
	);
end

$$

delimiter ;
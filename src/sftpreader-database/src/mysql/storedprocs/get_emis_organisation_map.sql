delimiter $$

drop procedure if exists get_emis_organisation_map;

$$

create procedure get_emis_organisation_map
(
	guid varchar(100)
)
begin
	select 
		e.guid,
		e.name,
		e.ods_code
	from emis_organisation_map e
	where e.guid = guid;
end

$$

delimiter ;

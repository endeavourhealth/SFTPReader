delimiter $$

drop procedure if exists add_batch_split;

$$

create procedure add_batch_split
(
	batch_id int,
	configuration_id varchar(100),
	local_relative_path varchar(1000),
	organisation_id varchar(100)
)
begin
	insert into batch_split
	(
		batch_id,
		configuration_id,
		local_relative_path,
		organisation_id
	)
	values
	(
		batch_id,
		configuration_id,
		local_relative_path,
		organisation_id
	);
end

$$

delimiter ;

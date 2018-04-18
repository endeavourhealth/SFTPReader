delimiter $$

drop procedure if exists add_unknown_file;

$$

create procedure add_unknown_file
(
	configuration_id varchar(100),
	filename varchar(1000),
	remote_size_bytes bigint,
	remote_created_date datetime
)
begin
	if not exists
	(
		select *
		from unknown_file u
		where u.configuration_id = configuration_id
		and u.filename = filename
	)
	then
		insert into unknown_file
		(
			configuration_id,
            insert_date,
			filename,
			remote_created_date,
			remote_size_bytes
		)
		values
		(
			configuration_id,
            now(),
			filename,
			remote_created_date,
			remote_size_bytes
		);
	end if;
end

$$

delimiter ;

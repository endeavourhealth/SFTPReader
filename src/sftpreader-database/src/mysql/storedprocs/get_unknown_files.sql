delimiter $$

drop procedure if exists get_unknown_files;

$$

create procedure get_unknown_files
(
	configuration_id varchar(100)
)
begin
	select
		u.unknown_file_id,
		u.insert_date,
		u.filename,
		u.remote_created_date,
		u.remote_size_bytes
	from unknown_file u
	where u.configuration_id = configuration_id;
end

$$

delimiter ;

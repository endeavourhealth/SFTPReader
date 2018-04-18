delimiter $$

drop procedure if exists set_file_as_downloaded;

$$

create procedure set_file_as_downloaded
(
	batch_file_id integer,
	local_size_bytes bigint
)
begin
	update batch_file f
	set
		f.is_downloaded = true,
		f.download_date = now(),
		f.local_size_bytes = local_size_bytes
	where f.batch_file_id = batch_file_id;
end

$$

delimiter ;

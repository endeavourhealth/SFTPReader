delimiter $$

drop procedure if exists set_file_as_decrypted;

$$

create procedure set_file_as_decrypted
(
	batch_file_id integer,
	decrypted_filename varchar(1000),
	decrypted_size_bytes bigint
)
begin
	update batch_file f
	set
		f.is_decrypted = true,
		f.decrypted_filename = decrypted_filename,
		f.decrypt_date = now(),
		f.decrypted_size_bytes = decrypted_size_bytes
	where f.batch_file_id = batch_file_id;
end

$$

delimiter ;

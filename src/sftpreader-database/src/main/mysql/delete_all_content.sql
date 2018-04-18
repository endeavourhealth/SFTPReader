delimiter $$

drop procedure if exists reset_all_content;

$$

create procedure reset_all_content ()
begin

	delete from unknown_file;
	delete from notification_message;
	delete from batch_split;
	delete from batch_file;
	delete from batch;

end

$$

delimiter ;

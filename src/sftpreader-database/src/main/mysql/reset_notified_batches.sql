delimiter $$

drop procedure if exists reset_notified_batches;

$$

create procedure reset_notified_batches ()
begin


	update batch_split
	set
		have_notified = false,
		notification_date = null;

end

$$

delimiter ;

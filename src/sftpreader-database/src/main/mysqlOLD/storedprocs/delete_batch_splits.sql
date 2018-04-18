delimiter $$

drop procedure if exists delete_batch_splits;

$$

create procedure delete_batch_splits
(
	batch_id integer
)
begin
	set SQL_SAFE_UPDATES = 0;

	delete bs
    from batch_split bs
	where bs.batch_id = batch_id;
    
    set SQL_SAFE_UPDATES = 1;
end

$$

delimiter ;

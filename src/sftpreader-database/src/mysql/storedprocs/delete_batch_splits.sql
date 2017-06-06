delimiter $$

drop procedure if exists delete_batch_splits;

$$

create procedure delete_batch_splits
(
	batch_id integer
)
begin
	delete bs
    from log.batch_split bs
	where bs.batch_id = batch_id;
end

$$

delimiter ;

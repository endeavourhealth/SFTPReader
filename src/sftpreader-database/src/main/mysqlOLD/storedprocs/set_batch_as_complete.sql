delimiter $$

drop procedure if exists set_batch_as_complete;

$$

create procedure set_batch_as_complete
(
	batch_id integer,
	sequence_number integer
)
begin
	update batch b
	set
		b.sequence_number = sequence_number,
		b.is_complete = true,
		b.complete_date = now()
	where b.batch_id = batch_id
	and b.is_complete = false;
end

$$

delimiter ;


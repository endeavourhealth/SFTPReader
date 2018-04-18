
-- run to get rid of any previous version with different signature
drop function if exists log.set_batch_as_complete(integer, integer);

create or replace function log.set_batch_as_complete
(
	_batch_id integer
)
returns void
as $$

	update log.batch
	set
		is_complete = true,
		complete_date = date_trunc('second', now()::timestamp)
	where batch_id = _batch_id
	and is_complete = false;
	
$$ language sql;

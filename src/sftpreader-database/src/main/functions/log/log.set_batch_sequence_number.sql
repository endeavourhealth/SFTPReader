
create or replace function log.set_batch_sequence_number
(
	_batch_id integer,
	_sequence_number integer
)
returns void
as $$

	update log.batch
	set
		sequence_number = _sequence_number
	where batch_id = _batch_id;

$$ language sql;


create or replace function log.get_last_complete_batch
(
	_configuration_id varchar
)
returns setof refcursor
as $$
declare
	_batch_ids integer[];
begin

	select
		array_agg(b.batch_id) into _batch_ids
	from log.batch b
	where b.configuration_id = _configuration_id
	and sequence_number =
	(
		select max(sequence_number)
		from log.batch
		where configuration_id = _configuration_id
		and is_complete = true
	);

	return query
	select
	  * 
	from log.get_batches(_batch_ids);

end;
$$ language plpgsql;
